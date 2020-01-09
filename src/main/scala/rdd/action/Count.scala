package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 创建一个RDD，统计该RDD的条数
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 */
object Count {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Count")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(1 to 10, 2)

    val count = rdd.count()

    println(count)
  }
}
