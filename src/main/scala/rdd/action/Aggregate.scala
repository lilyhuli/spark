package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
 * @projectName spark
 * @description: 需求：返回该RDD排序后的前n个元素组成的数组
 * @author tangd-a
 */
object Aggregate {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Aggregate")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(Array(2,5,4,6,8,3))

    rdd.takeOrdered(3).foreach(println)

  }
}
