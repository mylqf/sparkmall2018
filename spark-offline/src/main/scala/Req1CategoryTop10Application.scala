import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.lqf.sparkmall2018.common.model.{CategoryTop10, UserVisitAction}
import com.lqf.sparkmall2018.common.util.{ConfigUtil, SparkmallUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 得到 点击（）点单  支付 品类top10
  * 思路 通过hive use_visit_action 查询数据 得到DataFrame 转为DataSet[] 再转为rdd[]
  * (id_click)
  */

/**
  * 需求1 ： 获取点击、下单和支付数量排名前 10 的品类
  */
object Req1CategoryTop10Application {


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //引入隐式转换
    import spark.implicits._
    spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
    val sql = "select * from user_visit_action where 1 = 1"
    val sqlBuilder = new StringBuilder(sql)
    val startDate = ConfigUtil.getCondValue("startDate")
    val endDate = ConfigUtil.getCondValue("endDate")
    if (SparkmallUtils.isNotEmpty(startDate)) {
      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
    }
    if (SparkmallUtils.isNotEmpty(endDate)) {
      sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
    }
    val actionLogDF = spark.sql("select * from user_visit_action where action_time>='2018-11-01' and action_time<='2018-12-28'")
    //val rdd = actionLogDF.rdd
    val actionLogRDD = actionLogDF.as[UserVisitAction].rdd
    //println(actionLogRDD.count())
    //    4.2 对数据进行统计分析
    //    4.2.1 将品类聚合成不同的属性数据（category, sumClick）,(category, sumOrder),(Category, sumPay)
    (1,2)

    // 使用累加器聚合数据
    // 声明累加器
    val categoryCountAccumulator = new CategoryCountAccumulator
    //注册累加器
    spark.sparkContext.register(categoryCountAccumulator)
    actionLogRDD.foreach(
      actionLog => {
        if (actionLog.click_category_id != -1) {
          //10_click
          categoryCountAccumulator.add(actionLog.click_category_id + "_click")
          //10_order
        } else if (actionLog.order_category_ids != null) {
          val ids = actionLog.order_category_ids.split(",")
          ids.foreach(id=>{
            categoryCountAccumulator.add(id+"_order")
          })
        } else if (actionLog.pay_category_ids != null) {
          val ids = actionLog.pay_category_ids.split(",")
          //10_pay
          ids.foreach(id => {
            categoryCountAccumulator.add(id + "_pay")
          })

        }

      }

    )
    // (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
    val categorySumMap = categoryCountAccumulator.value
    //4.2.2 将聚合的数据融合在一起（category, (sumClick, sumOrder, sumPay)）
    val statMap = categorySumMap.groupBy {
      case (k, v) => {
        k.split("_")(0)
      }
    }

    val taskId = UUID.randomUUID().toString
    val listData = statMap.map {
      case (categoryId, map) => {
        CategoryTop10(
          taskId,
          categoryId,
          map.getOrElse(categoryId + "_click", 0L),
          map.getOrElse(categoryId + "_order", 0L),
          map.getOrElse(categoryId + "_pay", 0L)

        )

      }

    }.toList

    //4.2.3 将聚合的数据根据要求进行倒序排列，取前10条
    val top10Data = listData.sortWith((x, y) => {

      if (x.clickCount < y.clickCount) {
        false
      } else if (x.clickCount == y.clickCount) {
        if (x.orderCount < y.orderCount) {
          false
        } else if (x.orderCount == y.orderCount) {
          x.payCount > y.payCount
        } else {
          true
        }
      } else {
        true
      }
    }).take(10)
    println(top10Data)

//        case (left, right) => {
//          if (left.clickCount < right.clickCount) {
//            false
//          } else if (left.clickCount == right.clickCount) {
//            if (left.orderCount < right.orderCount) {
//              false
//            } else if (left.orderCount == right.orderCount) {
//              left.payCount > right.payCount
//            } else {
//              true
//            }
//          } else {
//            true
//          }
//        }




    //4.3 将分析结果保存到MySQL中
    //    4.3.1 将统计结果使用JDBC存储到Mysql中

    val driverClass = ConfigUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigUtil.getValueByKey("jdbc.url")
    var user = ConfigUtil.getValueByKey("jdbc.user")
    var password = ConfigUtil.getValueByKey("jdbc.password")

    Class.forName(driverClass)

    val connection: Connection = DriverManager.getConnection(url, user, password)

    val insertSQL = "insert into category_top10 values ( ?, ?, ?, ?, ? )"

    val pstat: PreparedStatement = connection.prepareStatement(insertSQL)


    top10Data.foreach(data=>{
      pstat.setObject(1, data.taskId)
      pstat.setObject(2, data.categoryId)
      pstat.setObject(3, data.clickCount)
      pstat.setObject(4, data.orderCount)
      pstat.setObject(5, data.payCount)

      pstat.executeUpdate()
    })

    pstat.close()
    connection.close()

    // 释放资源
    spark.stop()


  }

}


/**
  * 品类总和累加器 (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
  */
class CategoryCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

  var map=new mutable.HashMap[String,Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v)=map.getOrElse(v,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    map.foldLeft(other.value){
        case (xmap, (category, count)) => {
          xmap(category) = xmap.getOrElse(category, 0L) + count
          xmap
        }

    }
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}



