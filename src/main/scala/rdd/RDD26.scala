package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 创建两个pairRDD，并将key相同的数据聚合到一个迭代器。
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 */
object RDD26 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD26")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd1 = sc.makeRDD(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    val rdd2 = sc.makeRDD(Array((1,4),(2,5),(3,6)))

    rdd1.cogroup(rdd2).collect.foreach(println)
  }
}
