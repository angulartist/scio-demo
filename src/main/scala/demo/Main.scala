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
import org.joda.time
import org.joda.time.Duration

import scala.concurrent.Future

object Main {
  private val FIXED_WINDOW_DURATION: Duration = Duration.standardHours(1)

  // Write to text sink
  def writeToFile(in: SCollection[(String, Int)],
                  options: Options): Future[Tap[String]] =
    in.saveAsTextFile(
      path = options.getOutputPath,
      suffix = options.getOutputSuffix
    )

  // Define a fixed-time window
  val defaultFixedWindow: Window[DataEvent] = Window
    .into[DataEvent](FixedWindows.of(FIXED_WINDOW_DURATION))
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
    val ctx = ScioContext(options)

    // Ingest raw string events
    // INPUT -> SCollection[String]
    val events = ctx
      .textFile(path = options.getInputPath)

    // Decode, and get right values
    // SCollection[String] -> SCollection[DataEvent]
    val prepared: SCollection[DataEvent] = events
      .map[Either[io.circe.Error, DataEvent]] { e: String =>
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
    writeToFile(counted, options)

    ctx.close().waitUntilFinish()
  }
}
