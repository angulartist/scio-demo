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
import org.joda.time.{Duration, Instant}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object Main {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val FIXED_WINDOW_DURATION: Duration = Duration.standardHours(1)

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

    val events: SCollection[String] =
      sc.pubsubTopic[String](
        topic = options.getInputTopic,
        idAttribute = "eventId",
        timestampAttribute = "timestamp"
      )

    val prepared = events
      .transform("Decode Events") {
        _.map[Either[io.circe.Error, RSVPEvent]] { e: String =>
          decode[RSVPEvent](e)
        }.collect { case Right(value) => value }
      }

    val windowed: SCollection[RSVPEvent] =
      prepared
        .transform("Assign Fixed Window") {
          _.applyTransform(defaultFixedWindow)
        }

    windowed.withTimestamp.withWindow.map {
      e: ((RSVPEvent, Instant), Nothing) =>
        logger.info(s"pouet $e")
    }

    sc.close()
      .waitUntilFinish()
  }
}
