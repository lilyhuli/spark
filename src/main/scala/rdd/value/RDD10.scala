package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD10 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD10")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 16,4)

    //查看分区数
    println("分区数1："+rdd.partitions.size)
    //1. 作用：根据分区数，重新通过网络随机洗牌所有数据。
    //重新分区，分区就是分任务数，就是shuffle
    val coalesceRDD = rdd.coalesce(3)

    //查看分区数
    println("分区数2："+coalesceRDD.partitions.size)


  }
}
