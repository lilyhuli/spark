package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 创建一个PairRDD，统计每种key的个数
 * 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
 * @author tangd-a
 */
object CountByKey {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("countByKey")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)



    println( rdd.countByKey())
  }
}
