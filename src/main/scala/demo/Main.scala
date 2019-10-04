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
  private val OUTPUT_PATH: String = "results"
  private val OUTPUT_SUFFIX: String = ".bo"
  private val FIXED_WINDOW_DURATION: Duration = Duration.standardHours(1)

  def writeToFile(toto: SCollection[(String, Int)]): Future[Tap[String]] =
    toto.saveAsTextFile(path = OUTPUT_PATH, suffix = OUTPUT_SUFFIX)

  val defaultFixedWindow: Window[DataEvent] = Window
    .into[DataEvent](FixedWindows.of(FIXED_WINDOW_DURATION))
    .triggering(groupedWithinTrigger)
    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.ZERO)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (ctx, _) = ContextAndArgs(cmdlineArgs)

    /**
      * Set to true if running a streaming pipeline.
      * This will be automatically set to true if the
      * pipeline contains an Unbounded PCollection.
      */
    ctx.optionsAs[StreamingOptions].setStreaming(true)

    val events = ctx
      .textFile("./dataset/mock.txt")

    val prepared = events
      .map { e: String =>
        decode[DataEvent](e)
      }
      .filter { e =>
        e.isRight
      }
      .map[DataEvent] { e =>
        e.right.get
      }
      .timestampBy { e: DataEvent =>
        new time.Instant(e.timestamp)
      }

    val windowed = prepared.applyTransform(defaultFixedWindow)

    val summedEvents: SCollection[(String, Int)] =
      windowed
        .map[(String, Int)] { e: DataEvent =>
          (s"${e.userId}_${e.server}", e.experience)
        }
        .sumByKey

    writeToFile(summedEvents)

    ctx.close().waitUntilFinish()
  }
}
