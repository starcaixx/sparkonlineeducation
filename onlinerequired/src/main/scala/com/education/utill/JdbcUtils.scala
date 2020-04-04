package com.education.utill

import java.sql.Connection
import java.util
import java.util.Properties

import com.mysql.fabric.jdbc.FabricMySQLDriver

/**
  * @author star 
  * @create 2019-04-23 16:05 
  */
object JdbcUtils {

    import java.sql.{DriverManager, SQLException}
    //链表 --- 实现栈结构//链表 --- 实现栈结构

    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

    val driver = prop.getProperty("druid.driverClassName")
    val jdbcUrl =  prop.getProperty("druid.url")
    val jdbcUser = prop.getProperty("druid.username")
    val jdbcPassword = prop.getProperty("druid.password")
    private val dataSources = new util.LinkedList[Connection]()

    //初始化连接数量
    for(i<- 0 to 10){
        print("xxx")
        DriverManager.registerDriver(new FabricMySQLDriver)
        val con: Connection = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword)
        dataSources.add(con)
    }

    @throws[SQLException]
    def getConnection: Connection = { //取出连接池中一个连接
        val connection: Connection = dataSources.removeFirst()
        connection // 删除第一个连接返回
    }

    //将连接放回连接池
    def releaseConnection(conn: Connection):Unit = {
        dataSources.add(conn)
    }

}
