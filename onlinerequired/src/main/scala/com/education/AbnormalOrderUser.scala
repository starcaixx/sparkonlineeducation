package com.education

import java.util.ResourceBundle

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, _}


/**
  * @author star 
  * @create 2019-04-23 19:11 
  */
object AbnormalOrderUser {
    private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true").setAppName(this.getClass.getSimpleName)

        val driver = bundle.getString("druid.driverClassName")
        val jdbcUrl =  bundle.getString("druid.url")
        val jdbcUser = bundle.getString("druid.username")
        val jdbcPassword = bundle.getString("druid.password")
        val processingInterval: Long = bundle.getString("processingInterval").toLong
        // 设置jdbc
        Class.forName(driver)
        // 设置连接池
        ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)
        val streamContext = new StreamingContext(sparkConf,Seconds(processingInterval))
        val brokers: String = bundle.getString("brokers")
        val kafkaParam: Map[String, String] = Map[String, String](
            "metadata.broker.list" -> brokers,
            "auto.offset.reset" -> "largest",
            "enable.auto.commit"->"false"
        )

        //get offset
        val fromOffsets = DB.readOnly { implicit session =>
            sql"select topic, part_id, offset from topic_offset".
              map { r =>
                  TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
              }.list.apply().toMap
        }

        var offsetRanges = Array.empty[OffsetRange]

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamContext, kafkaParam, fromOffsets, messageHandler)

        kafkaDStream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }).filter(filterCompleteOrderData(_)).map(getCountryAndDate(_)).reduceByKey(_+_).filter(filterGT3(_)).foreachRDD(rdd=>{
            val tuples: Array[(String, Int)] = rdd.collect()

            DB.localTx{implicit session=>{
                tuples.foreach(msg=>{
                    val uid: String = msg._1
                    println(msg)
                    sql"""replace into unpayment_record(uid) values (${uid})""".executeUpdate().apply()
                })

                for (elem <- offsetRanges) {
                    sql"""update unpayment_topic_offset set offset = ${elem.untilOffset} where topic = ${elem.topic} and part_id = ${elem.partition}""".update.apply()
                }
            }}
            //start transcation
            /*partition.foreach(msg => {
                val dt = msg._1._1
                val province = msg._1._2
                val cnt: Long = msg._2.toLong

                sql"""replace into vip_increment_analysis(province,dt,cnt) values (${province},${cnt},${dt})""".executeUpdate().apply()
                println(msg)
            })
            for (elem <- offsetRanges) {
                println(elem.topic, elem.partition, elem.fromOffset, elem.untilOffset)
                sql"""update topic_offset set offset = ${elem.untilOffset} where topic = ${elem.topic} and part_id = ${elem.partition}""".update().apply()
            }*/
            //commit transcation
        })

        streamContext.start()
        streamContext.awaitTermination()
    }

    def filterGT3(tuple: (String, Int)): Boolean = {
        tuple._2>=3
    }

    def isVIP(str: String):Boolean = {
        DB.readOnly { implicit session =>
            sql"select uid from vip_users".
              map { r =>
                  r.string(1)
              }.list.apply()
        }.isEmpty
    }

    def filterCompleteOrderData(msg: (String, String)): Boolean = {
        val infos: Array[String] = msg._2.split("\t")
        infos != null && infos.length == 17 && "enterOrderPage".equals(infos(15)) && !isVIP(infos(0))
    }

    def getCountryAndDate(msg: (String, String)): (String, Int) = {
        val columns: Array[String] = msg._2.split("\t")
//        (18451,18451,F,2,0,Symbian,auto,4G,27.129.32.0,187****8451,1,300,1555603200,1555603300,1.0,endVideo,1555603200)
        println("active:" + msg)
        (columns(0), 1)
    }
}
