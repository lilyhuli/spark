package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD04 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD04")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))

    //flatMap 类似于map，但是每一个输入元素可以被映射为0或多个输出元素(所以func应该返回一个序列，而不是一个元素)
    val flatMapRDD = rdd.flatMap(1 to _)
    val array = flatMapRDD.collect()

    print(array)

    array.foreach(println)

  }
}
