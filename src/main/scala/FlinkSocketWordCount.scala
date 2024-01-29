import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkSocketWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create a socket source with the specified host and port
    val source = env.socketTextStream("localhost", 9999)
      .name("SocketTextStream")  // Add a name for better identification in the logs

    // Split the lines into words
    val words = source.flatMap(_.split(" ")).map(word => (word, 1))

    // Generate running word count
    val wordCounts = words.keyBy(0).timeWindow(Time.seconds(5)).sum(1)

    // Print the result to the console
    wordCounts.print().name("FlinkSocketWordCount")

    // Execute the Flink job
    env.execute("FlinkSocketWordCountJob")
  }
}
