package rdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD18 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD18")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val rdd = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    val size = rdd.partitions.size

    println("first: "+size)
    println("------------")
    val rdd2 = rdd.partitionBy(new HashPartitioner(2))

    println("second: "+ rdd2.partitions.size)










  }
}
