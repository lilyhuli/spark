package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 并将value添加字符串"$$"
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD24 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD24")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val array = Array((1,"a"),(1,"d"),(2,"b"),(3,"c"))

    val rdd = sc.makeRDD(array)

    rdd.mapValues(_+"$$").collect.foreach(println)

  }
}
