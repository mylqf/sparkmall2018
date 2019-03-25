import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.lqf.sparkmall2018.common.model.UserVisitAction
import com.lqf.sparkmall2018.common.util.{ConfigUtil, SparkmallUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Req3PageJumpApplication {


  def main(args: Array[String]): Unit = {



    //4.1 从Hive中获取数据
    //    4.1.1 使用SparkSQL来获取数据

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")

    // 构建SparkSql环境
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 引入隐式转换
    import spark.implicits._

    spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))

    var sql = "select * from user_visit_action where 1 = 1 "

    val sqlBuilder = new StringBuilder(sql)

    val startDate = ConfigUtil.getCondValue("startDate")
    val endDate = ConfigUtil.getCondValue("endDate")

    if ( SparkmallUtils.isNotEmpty(startDate) ) {
      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
    }
    if ( SparkmallUtils.isNotEmpty(endDate) ) {
      sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
    }

    val actionLogDF: DataFrame = spark.sql(sqlBuilder.toString)
    //    4.1.2将获取的数据转换为RDD
    val actionLogRDD: RDD[UserVisitAction] = actionLogDF.as[UserVisitAction].rdd

    // *********************************************************
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionLogRDD.groupBy(log=>log.session_id)

    //groupRDD.map((x,y)=>{(x,(y))})
    val zipPageCountRDD=groupRDD.mapValues{datas=>{

       val pageFlows=datas.toList.sortWith{
          (x,y)=>{x.action_time<y.action_time}
        }
      // 1 - 2 -3 - 4 - 5
      // 2 - 3- 4- 5
       val pageidFlows = pageFlows.map(_.page_id)
      // (1-2), (2-3)，(3-4), (4-5)
       val zipPageids = pageidFlows.zip(pageidFlows.tail)
//
//       zipPageids.map {
//         (x,y)=>{
//           (x + "_" + y,1)
//         }
//       }
      zipPageids.map {
        case (pid1, pid2) => {
          (pid1 + "_" + pid2, 1)
        }
      }

      }

    }

    // 4.4 统计拉链后的数据点击总次数（A）
    val flatZipToCountRDD: RDD[(String, Int)] = zipPageCountRDD.map(_._2).flatMap(x=>x)

    val targetIds = ConfigUtil.getCondValue("targetPageFlow").split(",")
    val targetPageFlows=targetIds.zip(targetIds.tail).map{
      case (pid1,pid2)=>{
        pid1+"_"+pid2
      }
    }
    val filterZipToCountRDD=flatZipToCountRDD.filter{

      case (pageflow,count)=>{
        targetPageFlows.contains(pageflow)
      }
    }
    val reduceZipToSumRDD = filterZipToCountRDD.reduceByKey(_+_)

    val filterActionRDD=actionLogRDD.filter(log=>{
      targetIds.contains(""+log.page_id)
    })
    val pageActionRDD = filterActionRDD.map(log=>{(log.page_id,1)}).reduceByKey(_+_)

    val pageActionMap = pageActionRDD.collect().toMap

    val taskId = UUID.randomUUID().toString

    reduceZipToSumRDD.foreach{

      case (pageflow,sum)=>{

        val pageid = pageflow.split("_")(0)
        val pageClickSumCount = pageActionMap(pageid.toLong)
        val rate=(sum.toDouble/pageClickSumCount*100).toInt
        println((taskId,pageflow,rate))

      }

    }


//    val driverClass = ConfigUtil.getValueByKey("jdbc.driver.class")
//    val url = ConfigUtil.getValueByKey("jdbc.url")
//    var user = ConfigUtil.getValueByKey("jdbc.user")
//    var password = ConfigUtil.getValueByKey("jdbc.password")
//
//    Class.forName(driverClass)
//
//    val connection: Connection = DriverManager.getConnection(url, user, password)
//
//    val insertSQL = "insert into category_top10 values ( ?, ?, ?, ?, ? )"
//
//    val pstat: PreparedStatement = connection.prepareStatement(insertSQL)
//
//
//    top10Data.foreach(data=>{
//      pstat.setObject(1, data.taskId)
//      pstat.setObject(2, data.categoryId)
//      pstat.setObject(3, data.clickCount)
//      pstat.setObject(4, data.orderCount)
//      pstat.setObject(5, data.payCount)
//
//      pstat.executeUpdate()
//    })
//
//    pstat.close()
//    connection.close()
//
//    // 释放资源
//    spark.stop()


  }

}
