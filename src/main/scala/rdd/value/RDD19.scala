package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD19 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD19")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRdd = sc.makeRDD(words).map(word =>(word,1))

    val group = wordPairsRdd.groupByKey()

    val result = group.collect()

    result.foreach(println)
    val res2 = group.map(t => (t._1, t._2.sum))
    println("---------")

    res2.collect.foreach(println)
  }
}
