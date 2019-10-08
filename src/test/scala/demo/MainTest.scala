package demo

import com.spotify.scio.testing._
import com.spotify.scio.values.WindowOptions
import demo.WindowParams.groupedWithinTrigger
import org.apache.beam.sdk.transforms.windowing.{
  IntervalWindow,
  TimestampCombiner
}
import org.apache.beam.sdk.values.TimestampedValue
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

import scala.util.Random

class MainTest extends PipelineSpec {
  private val random = new Random()
  private case class Topic(urlkey: String, topic_name: String)

  private case class Group(group_city: String,
                           group_country: String,
                           group_topics: List[Topic])

  private case class RSVPEvent(eventId: Int, timestamp: Long, group: Group)

  private val baseTime: Instant = new Instant(0)

  private val firstGroup = Group(
    "Paris",
    "France",
    List(Topic("cpp", "C++ Fans"), Topic("c-sharp", "C# Fans"))
  )

  private val secondGroup = Group(
    "London",
    "UK",
    List(
      Topic("php-haters", "Folks who hate PHP a lot!"),
      Topic("tee-sipping", "We're sipping your tears bruh")
    )
  )

  private val thirdGroup = Group(
    "Madrid",
    "Spain",
    List(
      Topic("c-sharp", "C# Fans"),
      Topic("angulartist", "Just Angulartists"),
      Topic("php-haters", "Folks who hate PHP a lot!"),
      Topic("tee-sipping", "We're sipping your tears bruh")
    )
  )

  private def event(eventId: Int,
                    group: Group,
                    baseTimeOffset: Duration): TimestampedValue[RSVPEvent] = {

    val timestamp = baseTime.plus(baseTimeOffset)

    TimestampedValue.of(
      RSVPEvent(eventId, timestamp.getMillis, group),
      timestamp
    )
  }

  "LeaderBoard.calculateTeamScores" should "work with on time elements" in {
    val stream = testStreamOf[RSVPEvent]
      .advanceWatermarkTo(baseTime)
      .addElements(
        event(1, firstGroup, Duration.standardSeconds(10)),
        event(2, secondGroup, Duration.standardSeconds(40))
      )
      //    .advanceWatermarkTo(baseTime.plus(Duration.standardSeconds(60)))
      //    .addElements(event(3, thirdGroup, Duration.standardSeconds(80)))
      .advanceWatermarkToInfinity

    val windowDuration = Duration.standardMinutes(5)

    val currentWindow = new IntervalWindow(baseTime, windowDuration)

    runWithContext { sc =>
      val pipeline = sc
        .testStream(stream)
        .transform("Filter and Flatten Topics") {
          _.filter { x: RSVPEvent =>
            x.group.group_topics.nonEmpty
          }.flatMap { x: RSVPEvent =>
            x.group.group_topics
          }
        }
        .transform("Assign a fixed-time window") {
          _.withFixedWindows(
            duration = Duration.standardMinutes(5),
            options = WindowOptions(
              trigger = groupedWithinTrigger,
              accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
              allowedLateness = Duration.ZERO,
              timestampCombiner = TimestampCombiner.END_OF_WINDOW
            )
          )
        }
        .transform("Sum topics per key") {
          _.map[(String, Int)] { x: Topic =>
            (s"${x.urlkey}", 1)
          }.sumByKey
        }

      pipeline should inOnTimePane(currentWindow) {
        containInAnyOrder(
          Seq(("cpp", 1), ("c-sharp", 1), ("php-haters", 1), ("tea-sipping", 1))
        )
      }
    }
  }
}
