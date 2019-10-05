package demo

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import demo.WindowParams.groupedWithinTrigger
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.{
  FixedWindows,
  TimestampCombiner,
  Window
}
import org.joda.time
import org.joda.time.Duration

import scala.concurrent.Future

object Main {
  // Some vars
  private val OUTPUT_PATH: String = "results"
  private val OUTPUT_SUFFIX: String = ".bo"
  private val FIXED_WINDOW_DURATION: Duration = Duration.standardHours(1)

  // Write to text sink
  def writeToFile(in: SCollection[(String, Int)]): Future[Tap[String]] =
    in.saveAsTextFile(path = OUTPUT_PATH, suffix = OUTPUT_SUFFIX)

  // Define a fixed-time window
  val defaultFixedWindow: Window[DataEvent] = Window
    .into[DataEvent](FixedWindows.of(FIXED_WINDOW_DURATION))
    .triggering(groupedWithinTrigger)
    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.ZERO)

  // Handle the context
  def main(cmdlineArgs: Array[String]): Unit = {
    val (ctx, _) = ContextAndArgs(cmdlineArgs)

    /**
      * Set to true if running a streaming pipeline.
      * This will be automatically set to true if the
      * pipeline contains an Unbounded PCollection.
      */
    ctx.optionsAs[StreamingOptions].setStreaming(true)

    // Ingest raw string events
    // INPUT -> SCollection[String]
    val events = ctx
      .textFile("./dataset/mock.txt")

    // Decode, and get right values
    // SCollection[String] -> SCollection[DataEvent]
    val prepared: SCollection[DataEvent] = events
      .map { e: String =>
        decode[DataEvent](e)
      }
      .collect { case Right(value) => value }

    // Assign a fixed-time window and assign the event-time
    // SCollection[DataEvent] -> SCollection[DataEvent]
    val windowed: SCollection[DataEvent] =
      prepared
        .timestampBy { e: DataEvent =>
          new time.Instant(e.timestamp)
        }
        .applyTransform(defaultFixedWindow)

    // Extract compound key, build key/value pairs and sum within that key
    // SCollection[(String, Int)] -> SCollection[(String, Int)]
    val counted: SCollection[(String, Int)] =
      windowed
        .map[(String, Int)] { e: DataEvent =>
          (s"${e.userId}_${e.server}", e.experience)
        }
        .sumByKey

    // Write to sink
    // SCollection[(String, Int)] -> OUTPUT
    writeToFile(counted)

    ctx.close().waitUntilFinish()
  }
}
