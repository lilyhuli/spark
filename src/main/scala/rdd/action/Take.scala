package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 返回一个由RDD的前n个元素组成的数组
 * @projectName spark
 * @description: 需求：创建一个RDD，统计该RDD的条数
 * @author tangd-a
 */
object Take {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Take")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(Array(2,5,4,6,8,3))

    rdd.take(3).foreach(println)

  }
}
