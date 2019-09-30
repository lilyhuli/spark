import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD09 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(1,2,1,5,2,9,6,1))

    val distinct = rdd.distinct().collect()

    val result = rdd.sample(true, 0.4, 2).collect()

    result.foreach(println)

    println("------------------")
    val result1 = rdd.sample(false, 0.4, 2).collect()

    result1.foreach(println)

  }
}
