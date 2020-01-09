package rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD21 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD21")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val list = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))

    val rdd = sc.makeRDD(list,3)


    val agg = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    agg.collect().foreach(println)
  }
}
