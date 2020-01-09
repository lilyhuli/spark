package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD01 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("WordCount")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)

    val rddStr = sc.makeRDD(Array("s","f","u","c","t"))
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))

    val array = rdd.collect()

    val arrayStr = rddStr.collect()

    //array.foreach(print)

    arrayStr.foreach(print)

  }
}
