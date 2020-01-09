package rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 作用：折叠操作，aggregate的简化操作，seqop和combop一样。
 * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 * 这个函数最终返回的类型不需要和RDD中元素类型一致。
 * 需求：创建一个RDD，将所有元素相加得到结果
 * @author tangd-a
 */
object Fold {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Fold")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)
    //分区

    val rdd = sc.makeRDD(1 to 10,2)



    println( rdd.fold(0)(_+_))
  }
}
