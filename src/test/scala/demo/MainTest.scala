package demo

import com.spotify.scio.testing._
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.{AfterWatermark, IntervalWindow}
import org.apache.beam.sdk.values.TimestampedValue
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

class MainTest extends PipelineSpec {
  private val WINDOW_DURATION: Duration = Duration.standardMinutes(2)
  private val BASE_TIME: Instant = new Instant(0)

  private def sampleEvent(
    record: String,
    baseTimeOffset: Duration
  ): TimestampedValue[String] = {

    TimestampedValue.of(record, BASE_TIME.plus(baseTimeOffset))
  }

  "Main" should "work with on time elements" in {
    val stream = testStreamOf[String]
      .advanceWatermarkTo(BASE_TIME)
      .addElements(
        // Sample Event
        sampleEvent(record = "Cat Cat Dog", Duration.standardSeconds(10)),
        // Sample Event
        sampleEvent(record = "Dog Bird Cat", Duration.standardSeconds(20)),
        // Empty Line
        sampleEvent(record = "", Duration.standardSeconds(30))
      )
      // Advance Watermark to the end of the window
      .advanceWatermarkTo(BASE_TIME.plus(Duration.standardMinutes(3)))
      .addElements(
        // After end of the window
        sampleEvent(record = "Dog", Duration.standardMinutes(4))
      )
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val pipeline = sc
        .testStream(stream)
        .transform("Filter Words") {
          _.map { x: String =>
            x.trim
          }.filter { x: String =>
            x.nonEmpty
          }
        }
        .transform("Tokenize Words") {
          _.flatMap(
            _.split("[^a-zA-Z']+")
              .filter(_.nonEmpty)
          )
        }
        .transform("Assign a fixed-time window") {
          _.withFixedWindows(
            duration = WINDOW_DURATION,
            options = WindowOptions(
              trigger = AfterWatermark.pastEndOfWindow(),
              accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
              allowedLateness = Duration.standardMinutes(2)
            )
          )
        }
        .transform("Pair Word With One") {
          _.map[(String, Int)] { x: String =>
            (x, 1)
          }.sumByKey
        }

      pipeline should inFinalPane(
        new IntervalWindow(BASE_TIME, WINDOW_DURATION)
      ) {
        containInAnyOrder(Seq(("Cat", 3), ("Dog", 2), ("Bird", 1)))
      }
    }
  }
}
