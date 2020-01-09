import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 统计出每一个省份广告被点击次数的TOP3
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/9/2917:50
 */
object Practice {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf，并设置app名称
    val conf = new SparkConf().setAppName("Practice")

    //2创建SparkContext，该对象是提交SparkApp的入口
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    val lines = sc.textFile("in\\agent.log")

    //3按照最小粒度聚合
    val provinceAdToOne = lines.map{
      x => val fields = x.split(" ")
        ((fields(1),fields(4)),1)
    }

    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
    val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)


    //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))

    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))

    val provinceGroup = provinceToAdSum.groupByKey()

    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数

    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    provinceAdTop3.collect().foreach(println)

    sc.stop()

  }
}
