import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author star 
  * @create 2019-04-21 19:54 
  */
object ChannelAccessStat {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ChannelAccessStat")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import sparkSession.implicits._

        sparkSession.sql(
            """
              |select channel,count(distinct uid) from
              |(select * from (select
              |uid,channel
              |from user_behavior where dt = 20190413) ub1
              |left join (select uid as uid1,channel as channel1 from user_behavior where dt < 20190413) ub2
              |on ub1.uid = ub2.uid1 where ub2.uid1 is null) tmp
              |group by tmp.channel;
            """.stripMargin)

        sparkSession.stop()
    }

}
