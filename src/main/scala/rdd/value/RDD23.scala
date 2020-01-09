package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 按照key的正序和倒序进行排序
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD23 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD23")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val array = Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd"))

    val rdd = sc.makeRDD(array)
    //正序
    rdd.sortByKey(true).collect().foreach(println)
    //倒叙
    rdd.sortByKey(false).collect().foreach(println)

  }
}
