package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD12 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD12")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val rdd = sc.makeRDD(List(2,1,3,4))

    val array = rdd.sortBy(x => x).collect()

    array.foreach(print)

    val array1 = rdd.sortBy(x => x % 3).collect()
    println("-------------------")

    array1.foreach(print)

  }
}
