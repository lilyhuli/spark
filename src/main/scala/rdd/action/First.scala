package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 返回RDD中的第一个元素
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 */
object First {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("First")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(1 to 10, 2)

    val first = rdd.first()

    println(first)
  }
}
