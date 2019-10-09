package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @title: StreamWordCount
  * @projectName spark
  * @description: TODO
  * @author tangd-a
  * @date 2019/10/913:51
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //1 初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2 初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3 通过监控端口创建DStream，一行一行的读进来的数据
    val lineSteams = ssc.socketTextStream("hadoop102", 9999)
    //4 将一行数据做切分，形成一个个单词
    val wordStreams = lineSteams.flatMap(_.split(" "))
    //5 将单词映射成元组（word，1）
    val wordAndOneStreams = wordStreams.map((_, 1))
    //6 将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)
    //打印
    wordAndCountStreams.print()

    //启动 SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()



  }

}
