package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD05 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD05")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)

    //分区数55
    val rdd = sc.makeRDD(1 to 50000001,55)
    //glom 将每一个分区形成一个数组
    val glomRDD = rdd.glom().collect()

    println(glomRDD)

  }
}
