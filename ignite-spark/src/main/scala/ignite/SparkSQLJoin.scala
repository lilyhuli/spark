package ignite

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: IgniteRDDDemo
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/11/2813:44
 */
object SparkSQLJoin {

  //Create a custom class to represent the Customer
  case class Customer(customer_id: Int, name: String, state: String, zip_code: String, company_id: Int)

  case class Company(company_id: Int, name: String)

  def main(args: Array[String]): Unit = {

    //Load the Ignite cache
    val ignite : Ignite = Ignition.start
    val companyCache : IgniteCache[Integer,String] = ignite.getOrCreateCache("CompanyCache")

    companyCache.put(0,"GridGain")
    companyCache.put(1,"Apple")
    companyCache.put(2,"IBM")
    companyCache.put(3,"Oracle")
    companyCache.put(4,"Microsoft")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark Sql Join Example"))
    //Create the SQLContext first from the existing Spark Context
    val sqlContext = new SQLContext(sc)

    //Import statement to implicitly convert an RDD to a DataFrame
    import sqlContext.implicits._
    //create igniteRDD
    val companyCacheIgnite = new IgniteContext(sc,()=> new IgniteConfiguration()).fromCache("CompanyCache")

    // create company dataframe
    val dfCompany = sqlContext.createDataFrame(companyCacheIgnite.map(p => Company(p._1, p._2)))

    // Register DataFrame as a table.
    dfCompany.registerTempTable("company")

    // Display the content of DataFrame
    dfCompany.show()

    // Print the DF schema
    dfCompany.printSchema()

  }


}
