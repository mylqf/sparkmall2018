import com.lqf.sparkmall2018.common.model.KafkaMessage
import com.lqf.sparkmall2018.common.util.{DateUtil, MallKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Req5TimeAreaCityClickApplication {

  def main(args: Array[String]): Unit = {

    // 获取kafka中的数据

    val topic = "ads_new_log181018"

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackListApplication")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MallKafkaUtil.getKafkaStream(topic, ssc)

    val messageDStream = kafkaDStream.map(record => {

      val line = record.value()
      val datas = line.split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))

    })
    val messageMapDStream = messageDStream.map(message => {
      val date = DateUtil.getStringByTimestamp(message.ts.toLong, "yyyy-MM-dd")
      (date + ":" + message.area + ":" + message.city + ":" + message.adid, 1L)
    })
    //使用有状态RDD，将数据保存到CP，同时更新Redis
    ssc.sparkContext.setCheckpointDir("cp")
    val sumClickStream = messageMapDStream.updateStateByKey {
      case (seq, cp) => {
        val sum = cp.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }
    sumClickStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client=RedisUtil.getJedisClient
        for (elem <- datas) {

          client.hset("date:area:city:ads",elem._1,elem._2.toString)

        }
        client.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()


  }

}
