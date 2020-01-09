package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 返回一个由RDD的前n个元素组成的数组
 * @projectName spark
 * @description: 需求：返回该RDD排序后的前n个元素组成的数组
 * @author tangd-a
 */
object TakeOrdered {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("TakeOrdered")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(Array(2,5,4,6,8,3))

    rdd.takeOrdered(3).foreach(println)

  }
}
