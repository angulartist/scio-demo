package demo

import org.apache.beam.sdk.options.{Default, Description}

trait WindowedOptions {
  @Description("Input file path of the mocked data")
  @Default.String("./dataset/mock.data")
  def getInputPath: String
  def setInputPath(value: String): Unit

  @Description("Output file(s) path of the results")
  @Default.String("results")
  def getOutputPath: String
  def setOutputPath(value: String): Unit

  @Description("Output file(s) suffix")
  @Default.String(".bo")
  def getOutputSuffix: String
  def setOutputSuffix(value: String): Unit

  @Description(
    "Fixed number of shards to produce per window, or null for runner-chosen sharding"
  )
  @Default.Integer(1)
  def getNumShards: Integer
  def setNumShards(numShards: Integer): Unit
}
