package demo

import org.apache.beam.sdk.transforms.windowing.{
  AfterFirst,
  AfterPane,
  AfterProcessingTime,
  Repeatedly
}
import org.joda.time.Duration

object WindowParams {
  private val MIN_BATCH_ELEMENTS: Int = 20
  private val MAX_BATCH_WAIT_TIME_IN_MILLIS: Duration =
    Duration.standardMinutes(1)

  val groupedWithinTrigger: Repeatedly = Repeatedly.forever(
    AfterFirst.of(
      AfterPane.elementCountAtLeast(MIN_BATCH_ELEMENTS),
      AfterProcessingTime
        .pastFirstElementInPane()
        .plusDelayOf(MAX_BATCH_WAIT_TIME_IN_MILLIS)
    )
  )
}
