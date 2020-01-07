package ignite

import org.apache.ignite.Ignition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath
import org.apache.ignite.spark.IgniteDataFrameSettings


/**
 * @title: DFWriter
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/11/28 20:07
 */
object DFWriter {

  val CONFIG :String = "config/example-ignite.xml"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DFWriter")
      .master("local[*]")
      .config("spark.executor.instances", "2")
      .getOrCreate();

    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org.apache.ignite").setLevel(Level.OFF)

    val peopleDF = sparkSession.read.json(resolveIgnitePath("resources/people.json").getAbsolutePath())

    println("JSON file contents:")

    peopleDF.show

    println("Wrting DataFrame to Ignite.")

    peopleDF.write.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, CONFIG)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "people")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
      .save

    println("Done!")


    Ignition.stop(false);
  }

}
