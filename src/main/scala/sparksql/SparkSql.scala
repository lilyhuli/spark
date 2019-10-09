package sparksql

import org.apache.spark.sql.SparkSession
/**
  * @title: SparkSql
  * @projectName spark
  * @description: TODO
  * @author tangd-a
  * @date 2019/10/817:36
  */
object SparkSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val df = spark.read.json("data/people.json")
    //stdout
    df.show()


//    df.filter($"age">21).show()

    df.createOrReplaceTempView("persons")


    spark.sql("SELECT * FROM persons where age > 21").show()

    spark.stop()


  }

}
