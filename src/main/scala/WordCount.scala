import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)

    //3使用sc创建RDD并执行相应的transformation和action
    val lines = sc.textFile("in")

    //将一行一行的数据分解成一个一个的单词
    val words = lines.flatMap(_.split(" "))

    //为了统计方便，将单词数据进行的转换
    val wordToOne = words.map((_, 1))
    //对转换结构后的数据进行分组聚合
    val wordToSum = wordToOne.reduceByKey(_+_)
    //将统计结果采集后打印到控制台
    val result = wordToSum.collect()

    //println(result)

    result.foreach(println)

  }
}
