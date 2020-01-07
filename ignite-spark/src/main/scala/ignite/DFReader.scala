package ignite

import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * @title: DFReader
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/11/28 19:45
 */
object DFReader {

  val CONFIG: String = "config/example-ignite.xml"

  def main(args: Array[String]): Unit = {
    val ignite: Ignite = Ignition.start(CONFIG)


    val sparkSession = SparkSession.builder()
      .appName("DFReader")
      .master("local[*]")
      .config("spark.executor.instances", "2")
      .getOrCreate()


    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org.apache.ignite").setLevel(Level.OFF) 

    println("Reading data from Ignite table.")

    val peopleDF = sparkSession.read
      .format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "people")
      .load
    peopleDF.createOrReplaceTempView("people")

    val sqlDF = sparkSession.sql("SELECT * FROM PEOPLE")
    sqlDF.show()
    sqlDF.collect()
    System.out.println("Done!")

    Ignition.stop(false)










  }
}
