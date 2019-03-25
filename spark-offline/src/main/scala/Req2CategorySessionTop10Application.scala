import java.util.UUID

import com.lqf.sparkmall2018.common.model.{CategoryTop10, UserVisitAction}
import com.lqf.sparkmall2018.common.util.{ConfigUtil, SparkmallUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable



/**
  * 需求2 ： Top10 热门品类中 Top10 活跃 Session 统计
  */
object Req2CategorySessionTop10Application {

  def main(args: Array[String]): Unit = {

    //4.1 从Hive中获取数据
    //    4.1.1 使用SparkSQL来获取数据

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")

    // 构建SparkSql环境
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 引入隐式转换
    import spark.implicits._

    spark.sql("use "+ConfigUtil.getValueByKey("hive.database"))
    var sql="select * from user_visit_action where 1 = 1"
    val sqlBuilder=new StringBuilder(sql)
    val startDate = ConfigUtil.getValueByKey("startDate")
    val endDate = ConfigUtil.getValueByKey("endDate")
    if (SparkmallUtils.isNotEmpty(startDate)){
      sqlBuilder.append(" and action_time >='").append(startDate).append("'")
    }
    if (SparkmallUtils.isNotEmpty(endDate)){
      sqlBuilder.append(" and action_time <='").append(endDate).append("'")
    }
    val actionLogDF = spark.sql(sqlBuilder.toString())
    val actionLogRDD = actionLogDF.as[UserVisitAction].rdd
    //println(actionLogRDD.count())
    //    4.2 对数据进行统计分析
    //    4.2.1 将品类聚合成不同的属性数据（category, sumClick）,(category, sumOrder),(Category, sumPay)

    // 使用累加器聚合数据
    // 声明累加器
    val categoryCountAccumulator = new CategoryCountAccumulator
    // 注册累加器
    spark.sparkContext.register(categoryCountAccumulator)

    actionLogRDD.foreach(
      actionLog=>{
        if (actionLog.click_category_id != -1) {
          // 10_click
          categoryCountAccumulator.add(actionLog.click_category_id + "_click")
        } else if (actionLog.order_category_ids != null) {
          val ids = actionLog.order_category_ids.split(",")
          // 10_order
          ids.foreach(id=>{
            categoryCountAccumulator.add(id + "_order")
          })
        } else if (actionLog.pay_category_ids != null) {
          val ids = actionLog.pay_category_ids.split(",")
          // 10_pay
          ids.foreach(id=>{
            categoryCountAccumulator.add(id + "_pay")
          })
        }

      }
    )



    // (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
    val categorySumMap: mutable.HashMap[String, Long] = categoryCountAccumulator.value

    //4.2.2 将聚合的数据融合在一起（category, (sumClick, sumOrder, sumPay)）

    val statMap: Map[String, mutable.HashMap[String, Long]] = categorySumMap.groupBy {
      case (k, v) => {
        k.split("_")(0)
      }
    }

    val taskId = UUID.randomUUID().toString


    val listData: List[CategoryTop10] = statMap.map {
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
    val top10Data: List[CategoryTop10] = listData.sortWith {
      case (left, right) => {
        if (left.clickCount < right.clickCount) {
          false
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount < right.orderCount) {
            false
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            true
          }
        } else {
          true
        }
      }
    }.take(10)





  }

}
