package demo

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
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

object Main {
  // Logger
  private val logger = LoggerFactory.getLogger(this.getClass)
  // Some vars
  private val FIVE_MINUTES: Int = 5
  private val FIXED_WINDOW_DURATION: Duration =
    Duration.standardMinutes(FIVE_MINUTES)

  // Define a fixed-time window
  val defaultFixedWindow: Window[RSVPEvent] = Window
    .into[RSVPEvent](FixedWindows.of(FIXED_WINDOW_DURATION))
    .triggering(groupedWithinTrigger)
    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
    .discardingFiredPanes()
    .withAllowedLateness(Duration.ZERO)

  // BigQuery models
  @BigQueryType.toTable
  case class Result(topic_name: String, score: Int, timestamp: Instant)

  // Main function, used to define the pipeline options
  def main(cmdlineArgs: Array[String]): Unit = {
    PipelineOptionsFactory.register(classOf[Options])

    val options = PipelineOptionsFactory
      .fromArgs(cmdlineArgs: _*)
      .withValidation
      .as(classOf[Options])
    options.setStreaming(true)

    run(options)
  }

  // The actual pipeline
  def run(options: Options): Unit = {
    val sc = ScioContext(options)

    // Ingest Pub/Sub messages sent by the consumer
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
        .transform("Extract and flatten topics") {
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

    // Stream insert into BigQuery
    sumTopics
      .transform("Insert into BigQuery") {
        _.withTimestamp
          .map[Result] { x: ((String, Int), Instant) =>
            val (topic_score: (String, Int), timestamp: Instant) = x
            Result(topic_score._1, topic_score._2, timestamp)
          }
          .saveAsTypedBigQuery(
            tableSpec = options.getBigQueryTrends,
            createDisposition = CREATE_IF_NEEDED
          )
      }

    sc.run()
      .waitUntilFinish()
  }
}
