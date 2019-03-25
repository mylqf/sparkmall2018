import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object KakfaStreaming {

  //从ZK获取offset
  def getOffsetFromZookeeper(kafkaCluster: KafkaCluster, kafkaGroup: String, kafkaTopicSet: Set[String]) = {

    //创建Map存储Topic和分区对应的offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()

    //获取传入的Topic的所有分区
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(kafkaTopicSet)
    //如果成功获取到Topic所有分区
    if (topicAndPartitions.isRight){
      //获取分区数据
      val parttitions = topicAndPartitions.right.get
      //获取指定分区的offset
      val offsetInfo = kafkaCluster.getConsumerOffsets(kafkaGroup,parttitions)
      if (offsetInfo.isLeft){
        //如果没有offset信息则存储0
        for (top<-parttitions){
          topicPartitionOffsetMap+=(top->0L)
        }
      }else{

        //如果有offset信息 则存储offset
        val offsets = offsetInfo.right.get
        for ((top,offset)<-offsets)
          topicPartitionOffsetMap+=(top->offset)

      }
      topicPartitionOffsetMap.toMap
    }


  }

  //提交offset
  def offsetZookeeper(kafkaDStream:InputDStream[String],kafkaCluster: KafkaCluster,kafka_group:String)={

    kafkaDStream.foreachRDD{
      rdd=>
        //获取DStream中的offset信息
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //遍历每一个offset信息，并更新Zookeeper中的元数据
        for (offsets<- offsetsList) {
          val topicAndPartition = TopicAndPartition(offsets.topic,offsets.partition)
          val ack = kafkaCluster.setConsumerOffsets(kafka_group,Map((topicAndPartition,offsets.untilOffset)))
          if (ack.isLeft){
            println(s"error ${ack.left.get} ")
          }else{
            println(s"success${offsets.untilOffset}")
          }
        }
    }

  }

  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")
    //创建SteamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //kafka参数声明
    val brokers="master:9092,slave01:9092"
    val topic="first"
    val group="bigdata"
    val deserialization="org.apache.kafka.common.serialization.StringDeserializer"
    //定义kafka参数
    val kafkaPara=Map[String,String](ConsumerConfig.GROUP_ID_CONFIG->group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization)
    //创建KafkaCluster(维护offset)
    val kafkaCluster = new KafkaCluster(kafkaPara)
    //获取ZK中保存的offset
    val fromOffset = getOffsetFromZookeeper(kafkaCluster,group,Set(topic))
    //读取kafka数据创建DStream
//    val kafkaDStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPara,fromOffset, (x:MessageAndMetadata[String,String])=>x.message())
//    //数据处理
//    kafkaDStream.print
    //提交offset
//    offsetZookeeper(kafkaDStream,kafkaCluster,group )
    ssc.start()
    ssc.awaitTermination()


  }

}
