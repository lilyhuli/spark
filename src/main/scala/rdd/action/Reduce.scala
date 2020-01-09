package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 创建一个RDD，将所有元素聚合得到结果。
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 */
object Reduce {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Reduce")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(1 to 10, 2)

    val int = rdd.reduce(_ + _)
    println(int)

    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))

    val result = rdd2.reduce((x,y) => (x._1 + y._1,x._2+y._2))

    println(result)





  }
}
