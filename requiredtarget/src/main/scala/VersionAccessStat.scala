import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author star 
  * @create 2019-04-21 20:15 
  */
object VersionAccessStat {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ChannelAccessStat")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import sparkSession.implicits._

        sparkSession.sql(
            """
              |select
              |os,version,count(distinct uid)
              |from user_behavior where dt = 20190413
              |group by os,version
            """.stripMargin)

        sparkSession.stop()
    }

}
