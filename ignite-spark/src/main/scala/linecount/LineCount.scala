package linecount

import org.apache.spark.sql.SparkSession

/**
 * @title: LineCount
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2020/1/7 17:25
 */
object LineCount {
  def main(args: Array[String]): Unit = {
    if(args.length <1) {
      System.err.println("Usage: LineCount<file>")
      System.exit(1)
    }
    val logFile  = args(0)
    val sparkSession = SparkSession.builder().appName("Line count Application").getOrCreate()
    //Dataset
    val logData = sparkSession.read.textFile(logFile).cache()

    val numAs = logData.filter(s => s.contains("a")).count()
    val numBs = logData.filter(s => s.contains("b")).count()

    println("Lines with a:" + numAs +", lines with b: " +numBs)

    sparkSession.stop()

  }
}
