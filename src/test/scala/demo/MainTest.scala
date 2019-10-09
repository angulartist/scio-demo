package demo

import com.spotify.scio.testing._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}

class MainTest extends PipelineSpec {
  private val FIVE_MIN_WINDOW_DURATION: Duration = Duration.standardMinutes(5)
  private val BASE_TIME: Instant = new Instant(0)

  private def sampleEvent(
    record: String,
    baseTimeOffset: Duration
  ): TimestampedValue[String] = {

    TimestampedValue.of(record, BASE_TIME.plus(baseTimeOffset))
  }

  "Main" should "work with on time elements" in {
    val stream = testStreamOf[String]
      .addElements(
        sampleEvent(record = "Cat Cat Dog", Duration.standardSeconds(10)),
        sampleEvent(record = "Dog Bird Cat", Duration.standardSeconds(20)),
        sampleEvent(record = "", Duration.standardSeconds(30))
      )
      .addElements(sampleEvent(record = "Dog", Duration.standardMinutes(6)))
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
          _.withFixedWindows(duration = FIVE_MIN_WINDOW_DURATION)
        }
        .transform("Pair Word With One") {
          _.map[(String, Int)] { x: String =>
            (x, 1)
          }.sumByKey
        }

      // Debugging
      pipeline.withWindow.debug(prefix = "Stream")

      // Test first 5 min fixed window contains in any order
      pipeline should inWindow(
        new IntervalWindow(BASE_TIME, FIVE_MIN_WINDOW_DURATION)
      ) {
        containInAnyOrder {
          Seq(("Cat", 3), ("Dog", 2), ("Bird", 1))
        }
      }

      // Test second 5 min fixed window contains in any order
      pipeline should inWindow(
        new IntervalWindow(
          new Instant(FIVE_MIN_WINDOW_DURATION.getMillis),
          FIVE_MIN_WINDOW_DURATION
        )
      ) {
        containInAnyOrder {
          Seq(("Dog", 1))
        }
      }
    }
  }
}
