import java.time.{Instant, LocalDateTime, ZoneId}

/**
  * @author star 
  * @create 2019-04-20 16:17 
  */
object Test {

    def main(args: Array[String]): Unit = {
        val array = Array(1,2,3,4)

        println(array(1))

        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(1555171200),ZoneId.systemDefault())
        import java.time.format.DateTimeFormatter
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        val str: String = localDateTime.format(formatter)
    }
}
