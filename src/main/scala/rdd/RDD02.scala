package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD02 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))

    /*
    mapPartitions(func)
    类似于map，可以独立RDD
     */
    val mapRDD = rdd.mapPartitions(_.map(_ * 2))

    val mapRDD2 = rdd.mapPartitions(x => x.map(_ * 2 ))


    val array = mapRDD.collect()

    val array2 = mapRDD2.collect()

    //array.foreach(print)

    array2.foreach(print)

  }
}
