import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object  Practice {

  def main(args: Array[String]): Unit = {

    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    //2.读取数据生成RDD：TS，Province，City，User，AD
    val line = sc.textFile("E:\\IDEAWorkSpace\\SparkTest\\src\\main\\resources\\agent.log")


    //3.按照最小粒度聚合：((Province,AD),1)
    val provinceAdAndOne = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }

    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
    val provinceAdToSum = provinceAdAndOne.reduceByKey(_ + _)


    //provinceAdToSum.map(x=>x._1._1) (province,(ad,sum))
    val provinceToAdSum=provinceAdToSum.map{

      case (x,y)=>{
        (x._1,(x._2,y))
      }

    }
    //groupByKey
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()
    provinceGroup.mapValues{x=>x.toList.sortWith((x,y)=>x._2>y._2)}



  }

}
