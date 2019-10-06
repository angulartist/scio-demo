package demo

import org.apache.beam.sdk.options.{
  Default,
  Description,
  PipelineOptions,
  StreamingOptions
}

trait Options extends PipelineOptions with StreamingOptions {
  @Description("Input file path of the mocked data")
  @Default.String("./dataset/mock.txt")
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

  @Description("Pub/Sub input topic")
  @Default.String("projects/drawndom-app/topics/demo-scala")
  def getInputTopic: String
  def setInputTopic(value: String): Unit

  @Description("Pub/Sub output topic")
  @Default.String("projects/drawndom-app/topics/demo-scala-sink")
  def getOutputTopic: String
  def setOutputTopic(value: String): Unit
}
