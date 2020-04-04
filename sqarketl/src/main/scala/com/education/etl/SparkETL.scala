package com.education.etl

import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * @author star 
  */
object SparkETL {

    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println("usage:xxx input output")
            System.exit(1)
        }
        val input = args(0)
        val output = args(1)

        val sparkConf: SparkConf = new SparkConf().setAppName("SparkETL")

        val sc = new SparkContext(sparkConf)

        val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import session.implicits._

        val activeRDD = session.sparkContext.textFile(input)

        val activesRDD: RDD[ActiveInfo] = activeRDD.filter(_.split("\t").length == 17).mapPartitions(actives => {
            actives.map(active => {
                val infos: Array[String] = active.split("\t")
                ActiveInfo(infos(0), infos(1), infos(2), infos(3).toInt, infos(4).toInt, infos(5), infos(6), infos(7), infos(8),
                    infos(9).replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2"),
                    infos(10), infos(11).toInt, infos(12).toInt, infos(13).toLong, infos(14), infos(15), infos(16).toLong)
            })
        })
        val resultRDD: RDD[(String, ActiveInfo)] = activesRDD.groupBy(active => {
            active.uid + ":" + active.eventKey + ":" + active.eventTime
        }).mapValues(actives => {
            actives.toList.take(1)
        }).values.flatMap(ls => ls).map(active => {
            val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(active.eventTime), ZoneId.systemDefault())
            import java.time.format.DateTimeFormatter
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
            (localDateTime.format(formatter), active)
        }).partitionBy(new HashPartitioner(7))

        resultRDD.saveAsHadoopFile(output,classOf[String],classOf[String],classOf[PairRDDMultipleTextOutputFormat])


        //        println(resultRDD.count())
        //        resultRDD.saveAsTextFile(output)
        //        resultRDD.toDF().write.format("text").save(output)

        session.stop()
    }

}

class PairRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any,Any]{
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
        val fileName: String = key.asInstanceOf[String]
        fileName
    }

    override def generateActualKey(key: Any, value: Any): AnyRef = {
        null
    }
}

case class ActiveInfo(uid: String, userName: String, gender: String, level: Int, isVip: Int, os: String, channel: String,
                      netConfig: String, ip: String, phone: String, videoId: String, videoLength: Int, startVideoTime: Long,
                      endVideoTime: Long, version: String, eventKey: String, eventTime: Long){
    override def toString: String = {
        uid+","+userName+","+gender+","+level+","+isVip+","+os+","+channel+","+netConfig+","+ip+","+phone+","+videoId+","+
        videoLength+","+startVideoTime+","+endVideoTime+","+version+","+eventKey+","+eventTime
    }
}

/*
uid STRING comment "用户唯一标识",
username STRING comment "用户昵称",
gender STRING comment "性别",
level TINYINT comment "1代表小学，2代表初中，3代表高中",
is_vip TINYINT comment "0代表不是会员，1代表是会员",
os STRING comment "操作系统:os,android等",
channel STRING comment "下载渠道:auto,toutiao,huawei",
net_config STRING comment "当前网络类型",
ip STRING comment "IP地址",
phone STRING comment "手机号码",
video_id INT comment "视频id",
video_length INT comment "视频时长，单位秒",
start_video_time BIGINT comment "开始看视频的时间缀，秒级",
end_video_time BIGINT comment "退出视频时的时间缀，秒级",
version STRING comment "版本",
event_key STRING comment "事件类型",
event_time BIGINT comment "事件发生时的时间缀，秒级"
 */