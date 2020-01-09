package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD13 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD13")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val rdd1 = sc.makeRDD(1 to 5)

    val rdd2 = sc.makeRDD(5 to 10)

    val rdd3 = rdd1.union(rdd2)

    val array = rdd3.collect()

    array.foreach(print)



  }
}
