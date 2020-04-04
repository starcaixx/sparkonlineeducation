package com.education

import java.time._
import java.time.format.DateTimeFormatter
import java.util.ResourceBundle

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * @author star 
  * @create 2019-04-22 14:42 
  */
object StatRegisterCount {
    private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
    val db = new City(this.getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)

    def main(args: Array[String]): Unit = {
        //// 参数检测
        if (args.length != 1) {
            println("useage:please input checkpointpath")
            System.exit(1)
        }
        val checkPoint = args(0)

        val driver = bundle.getString("druid.driverClassName")
        val jdbcUrl =  bundle.getString("druid.url")
        val jdbcUser = bundle.getString("druid.username")
        val jdbcPassword = bundle.getString("druid.password")

        // 设置jdbc
        Class.forName(driver)
        // 设置连接池
        ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

        val streamingContext: StreamingContext = StreamingContext.getOrCreate(checkPoint, () => getVipIncrementByCountry(checkPoint))

        streamingContext.start()
        streamingContext.awaitTermination()
    }


    def getVipIncrementByCountry(checkPoint: String): StreamingContext = {
        //define update
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            Some(values.sum + state.getOrElse(0))
        }

        val processingInterval: Long = bundle.getString("processingInterval").toLong
        val brokers: String = bundle.getString("brokers")
//        val topic: String = bundle.getString("topic")

//        val topicSet: Set[String] = topic.split(",").toSet

        //spark.streaming.stopGracefullyOnShutdown 优雅的关闭spark服务
        val sparkConf: SparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown", "true").setAppName(this.getClass.getSimpleName)

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        val kafkaParam: Map[String, String] = Map[String, String](
            "metadata.broker.list" -> brokers,
            "auto.offset.reset" -> "smallest"
        )

        //get offset
        val fromOffsets = DB.readOnly { implicit session =>
            sql"select topic, part_id, offset from topic_offset".
              map { r =>
                  TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
              }.list.apply().toMap
        }

        println("fromOffsets:" + fromOffsets)

        var offsetRanges = Array.empty[OffsetRange]

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParam, fromOffsets, messageHandler)

        val resultDS: DStream[((String, String), Int)] = kafkaDStream.transform(rdd => {
            println("================transform=============")
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }).filter(msg => {
            //过滤非法数据
            filterCompleteOrderData(msg)
        }).map(msg => {
            println("==========数据转换=========")
            //数据转换((date,add),1)
            getCountryAndDate(msg)
        }).updateStateByKey[Int](updateFunc).filter(filterTwoDaysData(_))

        resultDS.foreachRDD(rdd=>{
            println("run rdd....")
//            val partId: Int = TaskContext.getPartitionId()
//            val context: TaskContext = TaskContext.get()
            val tuples: Array[((String, String), Int)] = rdd.collect()
            DB.localTx{implicit session =>{
                tuples.foreach{
                    case ((dt,province),cnt)=>{
                        sql"""replace into vip_increment_analysis(province,dt,cnt) values (${province},${cnt},${dt})""".executeUpdate().apply()
                        println("data:"+(dt,province),cnt)
                    }
                }
                for (elem <- offsetRanges) {
                        sql"""update topic_offset set offset = ${elem.untilOffset} where topic = ${elem.topic} and part_id = ${elem.partition}""".update().apply()
                }
            }

            }
        })
        /*foreachRDD(rdd=>{
            //这里不能用foreachpartition，精确消费问题
            rdd.foreachPartition(partition => {
                //start transaction
                DB.localTx { implicit session =>{
                    partition.foreach(msg => {
                        val dt = msg._1._1
                        val province = msg._1._2
                        val cnt: Long = msg._2.toLong

                        sql"""replace into vip_increment_analysis(province,dt,cnt) values (${province},${cnt},${dt})""".executeUpdate().apply()
                        println(msg)
                    })
                    for (elem <- offsetRanges) {
                        println(elem.topic, elem.partition, elem.fromOffset, elem.untilOffset)
                        sql"""update topic_offset set offset = ${elem.untilOffset} where topic = ${elem.topic} and part_id = ${elem.partition}""".update().apply()
                    }
                }
                }
            })
        }*/

        //开启检查点
        streamingContext.checkpoint(checkPoint)
        //批处理时间的5 - 10倍
        kafkaDStream.checkpoint(Seconds(processingInterval * 10))
        streamingContext
    }
    def filterTwoDaysData(tuple: ((String, String), Int)): Boolean = {
        //            数据格式((date,add),1)
        val df = DateTimeFormatter.ofPattern("yyyyMMdd");
        val time: LocalDate = LocalDate.parse(tuple._1._1,df)

        Period.between(time, LocalDate.now()).getDays<=12
    }

    def getCountryAndDate(msg: (String, String)): ((String, String), Int) = {
        val columns: Array[String] = msg._2.split("\t")

//        println("active:" + msg)
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(columns(16).toLong), ZoneId.systemDefault())
        import java.time.format.DateTimeFormatter
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

        val cityInfo: CityInfo = db.findInfo(columns(8), "CN")
        var regionName = "未知"
        regionName = cityInfo.getRegionName
        ((localDateTime.format(formatter), regionName), 1)
    }

    def filterCompleteOrderData(msg: (String, String)): Boolean = {
        val infos: Array[String] = msg._2.split("\t")
        infos != null && infos.length == 17 && "completeOrder".equals(infos(15))
    }

    def demo(): Unit = {
        /*streamContext.sparkContext.setCheckpointDir("cp")
        val kafkaStream: ReceiverInputDStream[(String, String)] =
            KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
                streamContext,
                Map[String, String](
                    (ConsumerConfig.GROUP_ID_CONFIG -> "atguigu"),
                    (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
                    (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
                    ("zookeeper.connect" -> "192.168.88.43:2181,192.168.88.44:2181,192.168.88.44:2181")
                ),
                Map[String, Int]("user_behavior" -> 1),
                StorageLevel.MEMORY_ONLY)
        val resultDS: DStream[(String, Int)] = kafkaStream.filter(info => {
            val infos: Array[String] = info._2.split("\t")
            infos != null && infos.length == 17 && "completeOrder".equals(infos(15))
        }).map(active => {
            val columns: Array[String] = active._2.split("\t")

            println("active:" + active)
            val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(columns(16).toLong), ZoneId.systemDefault())
            import java.time.format.DateTimeFormatter
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
            val db = new City("F:\\git\\ipipfree.ipdb")

            val cityInfo: CityInfo = db.findInfo(columns(8), "CN")
            (localDateTime.format(formatter) + ":" + cityInfo.getRegionName, 1)
        }).groupByKey().mapValues(_.sum).updateStateByKey {
            case (countSeq, cache) => {
                val sumCount: Int = cache.getOrElse(0) + countSeq.sum
                Option(sumCount)
            }
        }
        //        (18451,18451,F,2,0,Symbian,auto,4G,27.129.32.0,187****8451,1,300,1555603200,1555603300,1.0,endVideo,1555603200)

        resultDS.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{
                for ((key,sum) <- datas) {
                    println(key+":"+sum)
                }
            })
        })

        resultDS.print()*/
    }

}