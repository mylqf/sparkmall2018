import java.text.SimpleDateFormat
import java.util.Date

import com.lqf.sparkmall2018.common.model.KafkaMessage
import com.lqf.sparkmall2018.common.util.{MallKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求4 实时数据分析 广告黑名单实时统计
  */
object Req4BlackListApplication {

  def main(args: Array[String]): Unit = {

    //获取kafka中的数据
    val topic="ads_new_log181018"
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackListApplication")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MallKafkaUtil.getKafkaStream(topic,ssc)
    val messageDStream: DStream[KafkaMessage] = kafkaDStream.map(record => {
      val line = record.value()
      val datas = line.split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })
    messageDStream.print()
    val filterMessageDStream=messageDStream.filter(message=>{
      val client = RedisUtil.getJedisClient
      val flag = client.sismember("blackList",message.userid)
      client.close()
      !flag
    })
    filterMessageDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client1=RedisUtil.getJedisClient
        datas.foreach(message=>{
          val logDate=new Date(message.ts.toLong)
          val sdf=new SimpleDateFormat("yyyy-MM-dd")
          val keyField=sdf.format(logDate)+":"+message.userid+":"+message.adid
          client1.hincrBy("date:user:advert:clickcount",keyField,1)
          val sumClick = client1.hget("date:user:advert:clickcount",keyField).toLong
          if (sumClick>=100){
            client1.sadd("blackList",message.userid)
          }
        })
        client1.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()



  }

}
