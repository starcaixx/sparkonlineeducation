import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author star 
  */
object StudyFeedback {

    def main(args: Array[String]): Unit = {
        import org.apache.spark
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StudyFeedback")

        val sc: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import sc.implicits._
        sc.sql("use dwd;")
        val ResultDF: DataFrame = sc.sql(
            """
select sum(watch_video_count),sum(complete_video_count) from
(select
count(distinct uid) watch_video_count,0 as complete_video_count
from user_behavior where dt = 20190413 and event_key = 'startVideo'
union all
select
0,count(distinct uid)
from user_behavior where dt = 20190413 and event_key = 'endVideo' and end_video_time-start_video_time>=video_length) tmp;
            """.stripMargin)
        ResultDF.show()
        sc.stop()
    }

}
