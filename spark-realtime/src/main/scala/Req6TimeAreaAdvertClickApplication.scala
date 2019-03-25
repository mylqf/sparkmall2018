import com.lqf.sparkmall2018.common.model.KafkaMessage
import com.lqf.sparkmall2018.common.util.{DateUtil, MallKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

/**
  * 需求6 : 每天各地区 top3 热门广告
  */
object Req6TimeAreaAdvertClickApplication {

  def main(args: Array[String]): Unit = {

    // 获取kafka中的数据

    val topic = "ads_new_log181018"

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackListApplication")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MallKafkaUtil.getKafkaStream(topic, ssc)

    // 4.1 获取用户点击广告的数据
    val messageDStream=kafkaDStream.map(record=>{
      val line = record.value()
      val datas = line.split(" ")
      KafkaMessage(datas(0),datas(1),datas(2),datas(3),datas(4))
    })

    val messageMapDStream: DStream[(String, Long)] = messageDStream.map(message => {
      val date = DateUtil.getStringByTimestamp(message.ts.toLong, "yyyy-MM-dd")
      (date + ":" + message.area + ":" + message.city + ":" + message.adid, 1L)
    })

    ssc.sparkContext.setCheckpointDir("cp")
    val sumClickDStream: DStream[(String, Long)] = messageMapDStream.updateStateByKey {
      case (seq, cp) => {
        val sum = cp.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }
    sumClickDStream.foreachRDD(rdd=> {
      rdd.foreachPartition(datas => {
        val client: Jedis = RedisUtil.getJedisClient

        for ((key, sum) <- datas) {
          client.hset("date:area:city:ads", key, sum.toString)
        }

        client.close()
      })
    })


    // 4.1 获取需求5的数据：（ date:area:city:advert, sum ）
    // **************************************************
    // 需求6逻辑
    // 4.2 将数据进行转换：（ date:area:city:advert, sum ）（ date:area:advert, sum1 ）, （ date:area:advert, sum2 ）, （ date:area:advert, sum3 ）

    val sumClickMapDStream = sumClickDStream.map {
      case (key, sum) => {
        val keys = key.split(":")
        val newKey = keys(0) + ":" + keys(1) + ":" + keys(3)
        (newKey, sum)
      }
    }

    // 4.3 将转换后的数据进行聚合：（ date:area:advert, sumTotal ）
    val sumTotalClickDStream = sumClickDStream.reduceByKey(_+_)
    // 4.4 将聚合后的数据进行结构转换：（ date:area:advert, sumTotal ） （ (date:area）(advert, sumTotal )）
    val sumTotalClickMapDStream=sumTotalClickDStream.map{
      case(key,sum)=>{
        val keys = key.split(":")
        (keys(0)+":"+keys(1),(keys(2),sum))
      }
    }
    val top3DStream = sumTotalClickMapDStream.groupByKey().mapValues(datas => {
      datas.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })
    val top3MapDStream = top3DStream.map {
      case (key, list) => {
        (key, list.toMap)
      }
    }
    top3MapDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(datas=>{

       val client= RedisUtil.getJedisClient
        import org.json4s.JsonDSL._
        for ((key,map) <- datas) {
          val keys = key.split(":")
          val jsonString=JsonMethods.compact(JsonMethods.render(map))
          client.hset("top3_ads_per_day:"+keys(0),keys(1),jsonString)

        }
        client.close()

      })
    })



  }


}
