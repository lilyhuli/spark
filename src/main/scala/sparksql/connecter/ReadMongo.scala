package sparksql.connecter

import org.apache.spark.sql.SparkSession

/**
 * @title: ReadMongo
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/10/916:32
 */
object ReadMongo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MongoSparkConnector")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val uri = "mongodb://172.1.1.1:27017"

    val usrDF = spark.sql("")


  }
}
