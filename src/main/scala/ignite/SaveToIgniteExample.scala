//package ignite
//import org.apache.ignite.cache.CacheAtomicityMode
//import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
//import org.apache.ignite.configuration.CacheConfiguration
//import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
//import scala.collection.JavaConversions._
//
///**
// * @title: SaveToIgniteExample
// * @projectName spark
// * @description: TODO
// * @author tangd-a
// * @date 2019/11/28 16:48
// */
//
//class Job {
//
//
//  def work(): Unit = {
//    val sharedRDD: IgniteRDD[String, Row] = igniteContext.fromCache(igniteCacheName)
//    sharedRDD.clear()
//    val rowsRdd: RDD[Row] = sparkContext.parallelize(
//      Seq(
//        Row(1, 2),
//        Row(3, 4),
//        Row(5, 6)
//      )
//    )
//    val dfWrite = sparkSession.createDataFrame(
//      rowsRdd,
//      StructType(StructField("id", IntegerType)::StructField("val", IntegerType)::Nil))
//
//
//    //Save dataFrame
//    SparkIgniteCache.set(sparkContext, dfWrite, igniteCacheName)
//
//    println(">>> #1 Collecting values stored in Ignite Shared RDD...")
//
//    sharedRDD.take(10).foreach(println)
//
//    //Get dataFrame
//    val (schema, igniteRDD) = SparkIgniteCache.get(sparkContext, igniteCacheName)
//    val rdd1: RDD[Row] = igniteRDD.map(_._2) //Getting Row from (String, Row)
//    val dfRead = parkSession.creatseDataFrame(rdd1, schema)
//
//    println(">>> #1 View current values in Ignite Shared RDD...")
//
//    dfRead.take(10).foreach(println)
//
//    dfRead.createOrReplaceTempView(igniteTableName)
//
//    println(">>> #1 Executing SQL query over Ignite Shared RDD...")
//
//    // Execute a SQL query over the Ignite Shared RDD.
//    val df = sparkSession.sql("select id, val from " + igniteTableName)
//
//    // Show ten rows from the result set.
//    df.show()
//
//  }
//  def close(): Unit = {
//    // Close IgniteContext on all workers.
//    igniteContext.close(true)
//
//    // Stop SparkContext.
//    sparkContext.stop()
//  }
//}
//
//
//object SaveToIgniteExample {
//
//  //Spark Configuration
//  private val conf: SparkConf = new SparkConf()
//    .setAppName("SaveToIgniteExample")
//    .setMaster("local[*]")
//    .set("spark.executor.instances", "2")
//
//  //Spark context
//  val sparkContext = new SparkContext(conf)
//
//  val sparkSession: SparkSession = SparkSession
//    .builder()
//    .config(conf)
//    .getOrCreate()
//
//  // Adjust the logger to exclude the logs of no interest.
//  Logger.getRootLogger.setLevel(Level.ERROR)
//  Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)
//
//  // Defines spring cache Configuration path.
//  private val CONFIG = SparkIgniteCache.CONFIG
//
//  val igniteCacheName: String = JDBCConnection.igniteCacheName
//  val igniteUrl: String = JDBCConnection.igniteUrl
//  val igniteTableName: String = JDBCConnection.igniteTableName
//
//  val igniteContext = new IgniteContext(sparkContext, CONFIG, false)
//
//  def init(): Unit = {
//    val jdbcJob = new JDBCConnection()
//    jdbcJob.work()
//  }
//
//
//  def main(args: Array[String]): Unit = {
//
//    for (cacheName <- igniteContext.ignite().cacheNames().toList) {
//      println("cache found:" + cacheName)
//    }
//    //    init()
//
//    val job = new Job()
//    job.work()
//    job.close()
//  }
//
//  }
//
//object SparkIgniteCache {
//  val CONFIG = "config/example-save-to-ignite.xml"
//  private val schemaCacheConfig = makeSchemaCacheConfig("schemas")
//
//  def set(sc: SparkContext, df: DataFrame, KEY: String): Unit = {
//    val ic = new IgniteContext(sc, CONFIG, false)
//    val sharedRDD = ic.fromCache[String, Row](KEY)
//    val rddSchemaCache = ic.ignite().getOrCreateCache(schemaCacheConfig)
//    rddSchemaCache.put(KEY + "_schema", df.schema)
//    sharedRDD.saveValues(df.rdd)
//  }
//
//
//  def get(sc: SparkContext, KEY: String): (StructType, IgniteRDD[String, Row]) = {
//    val ic = new IgniteContext(sc, CONFIG, false)
//    val rddSchemaCache = ic.ignite.getOrCreateCache(schemaCacheConfig)
//    (rddSchemaCache.get(KEY + "_schema"), ic.fromCache[String, Row](KEY))
//  }
//
//  private def makeSchemaCacheConfig(name: String) =
//    new CacheConfiguration[String, StructType](name)
//      .setAtomicityMode(CacheAtomicityMode.ATOMIC)
//      .setBackups(1)
//      .setAffinity(new RendezvousAffinityFunction(false, 1))
//}
