package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD14 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD14")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val rdd1 = sc.makeRDD(1 to 5)

    val rdd2 = sc.makeRDD(3 to 8)

    //subtract 计算差的一种函数，去除两个RDD中的相同的元素，不同的RDD将保留下来
    //差集
    //val array = rdd1.subtract(rdd2).collect()
    val array = rdd2.subtract(rdd1).collect()



    array.foreach(print)



  }
}
