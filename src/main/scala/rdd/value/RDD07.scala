package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD07 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD07")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array("xiaoming","xiaojiang","xiaohe","dazhi"))

    val filter = rdd.filter(_ .contains("xiao"))

    val filterResult = filter.collect()

    filterResult.foreach(println)

  }
}
