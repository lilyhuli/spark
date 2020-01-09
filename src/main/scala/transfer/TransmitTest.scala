package transfer

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: SeriTest
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2020/1/9 16:24
 */
object TransmitTest {
  def main(args: Array[String]): Unit = {
    //初始化配置信息 及sc
    val conf = new SparkConf().setAppName("TransmitTest")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("hadoop", "spark", "hive", "ignite"))
    val search = new Search("h")

    val match2 = search.getMatch2(rdd)

    match2.collect.foreach(println)



  }

}
