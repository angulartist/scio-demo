package demo

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, WindowOptions}
import demo.WindowParams.groupedWithinTrigger
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

object Main {
  private val FIVE_MINUTES: Int = 5
  private val FIXED_WINDOW_DURATION: Duration =
    Duration.standardMinutes(FIVE_MINUTES)

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

    // Processing steps
    events
      .transform("Decode Events") {
        _.map[Either[io.circe.Error, RSVPEvent]] { decode[RSVPEvent](_) }
          .collect { case Right(value) => value }
      }
      .transform("Assign Fixed Window") {
        _.withFixedWindows(
          duration = FIXED_WINDOW_DURATION,
          options = WindowOptions(
            trigger = groupedWithinTrigger,
            accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
            allowedLateness = Duration.ZERO,
            timestampCombiner = TimestampCombiner.END_OF_WINDOW
          )
        )
      }
      .transform("Filter and Flatten topics") {
        _.filter { _.group.group_topics.nonEmpty }
          .flatMap { _.group.group_topics }
      }
      .transform("Sum topics per key") {
        _.map[(String, Int)] { x: Topic =>
          (s"${x.urlkey}_${x.topic_name}", 1)
        }.sumByKey
      }
      .transform("Insert into BigQuery") {
        _.withTimestamp
          .map[Result] { x: ((String, Int), Instant) =>
            val (topic_score: (String, Int), timestamp: Instant) = x
            Result(topic_score._1, topic_score._2, timestamp)
          }
          .debug(prefix = "Before Insert")
//          .saveAsTypedBigQuery(
//            tableSpec = options.getBigQueryTrends,
//            createDisposition = CREATE_IF_NEEDED
//          )
      }

    sc.run()
      .waitUntilFinish()
  }
}
