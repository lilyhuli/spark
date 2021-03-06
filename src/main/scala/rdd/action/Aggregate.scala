package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
 * 作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
 * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 * 这个函数最终返回的类型不需要和RDD中元素类型一致。
 * 需求：创建一个RDD，将所有元素相加得到结果
 * @author tangd-a
 */
object Aggregate {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Aggregate")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(1 to 10,2)



    println( rdd.aggregate(0)(_+_,_+_))
  }
}
