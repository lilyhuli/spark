package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD08 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD08")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(1 to 10)
    //第一个参数 是否放回
    val result = rdd.sample(true, 0.4, 2).collect()

    result.foreach(println)

    println("------------------")
    val result1 = rdd.sample(false, 0.4, 2).collect()

    result1.foreach(println)

  }
}
