package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD20 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("RDD20")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区
    val list = List(("female",1),("male",5),("female",5),("male",2))

    val rdd = sc.makeRDD(list)
    //在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置。
    val reduce = rdd.reduceByKey((x, y) => x + y)
    //1. reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v].
    //2. groupByKey：按照key进行分组，直接进行shuffle。
    //3. 开发指导：reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑
    reduce.collect().foreach(println)
  }
}
