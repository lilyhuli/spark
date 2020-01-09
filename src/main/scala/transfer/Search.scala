package transfer

import org.apache.spark.rdd.RDD

/**
 * @title: 在实际开发中我们往往需要自己定义一些对于RDD的操作，
 *         那么此时需要主要的是，初始化工作是在Driver端进行的，
 *         而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，
 *         是需要序列化的。下面我们看几个例子：
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2020/1/9 16:04
 */
class Search(query: String)  {

  //过滤出包含字符串的数据
  def isMatch(s:String):Boolean = {
    s.contains(query)
  }
  //过滤出包含字符串的RDD
  def getMatch1(rdd :RDD[String]): RDD[String] ={
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd :RDD[String]): RDD[String] ={
     val query_ = this.query//将类连变量赋值给局部变量

    rdd.filter(x => x.contains(query_))
  }
}
