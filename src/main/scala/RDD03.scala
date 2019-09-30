import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object RDD03 {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))


    val indexRDD = rdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))

    val array = indexRDD.collect()

    array.foreach(println)

  }
}
