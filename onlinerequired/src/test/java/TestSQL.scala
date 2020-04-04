import java.time._
import java.time.format.DateTimeFormatter
import java.util.Properties

import kafka.common.TopicAndPartition
import scalikejdbc.DB
import scalikejdbc._
/**
  * @author star 
  * @create 2019-04-23 19:56 
  */
object TestSQL {

    def main(args: Array[String]): Unit = {

        val df = DateTimeFormatter.ofPattern("yyyyMMdd");
        val time: LocalDate = LocalDate.parse("20190415",df)


        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond("1555084800".toLong), ZoneId.systemDefault())

        import java.time.format.DateTimeFormatter
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        println(Period.between(time, LocalDate.parse(localDateTime.format(formatter).toString, df)).getDays)

        println(Period.between(time, LocalDate.now()).getDays)

        /*val prop = new Properties()
        prop.load(this.getClass.getClassLoader().getResourceAsStream("jdbc.properties"))
        val driver = prop.getProperty("druid.driverClassName")
        val jdbcUrl =  prop.getProperty("druid.url")
        val jdbcUser = prop.getProperty("druid.username")
        val jdbcPassword = prop.getProperty("druid.password")

        // 设置jdbc
        Class.forName(driver)
        // 设置连接池
        ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

        val fromOffsets = DB.readOnly { implicit session => sql"select topic, part_id, offset from topic_offset".
          map { r =>
              TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
          }.list.apply().toMap
        }

        println(fromOffsets)*/
    }

}
