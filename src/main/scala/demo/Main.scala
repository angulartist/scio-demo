package demo

import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import demo.WindowParams.groupedWithinTrigger
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{
  FixedWindows,
  TimestampCombiner,
  Window
}
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object Main {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val FIVE_MINUTES: Int = 5
  private val FIXED_WINDOW_DURATION: Duration =
    Duration.standardMinutes(FIVE_MINUTES)

  // Write to text sink
  def writeToFile(in: SCollection[(String, Int)],
                  options: Options): Future[Tap[String]] =
    in.saveAsTextFile(
      path = options.getOutputPath,
      suffix = options.getOutputSuffix
    )

  // Define a fixed-time window
  val defaultFixedWindow: Window[RSVPEvent] = Window
    .into[RSVPEvent](FixedWindows.of(FIXED_WINDOW_DURATION))
    .triggering(groupedWithinTrigger)
    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.ZERO)

  def main(cmdlineArgs: Array[String]): Unit = {
    PipelineOptionsFactory.register(classOf[Options])

    val options = PipelineOptionsFactory
      .fromArgs(cmdlineArgs: _*)
      .withValidation
      .as(classOf[Options])
    options.setStreaming(true)

    run(options)
  }

  // Handle the context
  def run(options: Options): Unit = {
    val sc = ScioContext(options)

    // Ingest Pub/Sub messages
    val events: SCollection[String] =
      sc.pubsubTopic[String](
        topic = options.getInputTopic,
        idAttribute = "eventId",
        timestampAttribute = "timestamp"
      )

    // Decode events and map them to a class
    val prepared: SCollection[RSVPEvent] =
      events
        .transform("Decode Events") {
          _.map[Either[io.circe.Error, RSVPEvent]] { x: String =>
            decode[RSVPEvent](x)
          }.collect { case Right(value) => value }
        }

    // Apply a fixed time window to the Unbounded SCollection
    val windowed: SCollection[RSVPEvent] =
      prepared
        .transform("Assign Fixed Window") {
          _.applyTransform(defaultFixedWindow)
        }

    // Extract group topics and flatten them
    val topics: SCollection[Topic] =
      windowed
        .transform("Assign Fixed Window") {
          _.flatMap { x: RSVPEvent =>
            x.group.group_topics
          }
        }

    // Sum topics per key (composite key url_name)
    val sumTopics: SCollection[(String, Int)] =
      topics
        .transform("Sum topics per key") {
          _.map[(String, Int)] { x: Topic =>
            (s"${x.urlkey}_${x.topic_name}", 1)
          }.sumByKey
        }

    // Logs
    sumTopics
      .map[(String, Int)] { e: (String, Int) =>
        logger.info(s"topic $e")
        e
      }

    sc.close()
      .waitUntilFinish()
  }
}
