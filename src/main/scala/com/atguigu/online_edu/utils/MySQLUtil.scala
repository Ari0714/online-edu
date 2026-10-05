package com.atguigu.online_edu.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util.Properties

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object MySQLUtil {

  def main(args: Array[String]): Unit = {

    println(queryList("SELECT * from offset_manager"))

  }

  def queryList(sql: String) = {

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val url: String = properties.getProperty("url")
    val username: String = properties.getProperty("username")
    val password: String = properties.getProperty("password")

    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection(url, username, password)

    val statement: PreparedStatement = conn.prepareStatement(sql)
    val resultSet: ResultSet = statement.executeQuery()
    val metaData: ResultSetMetaData = statement.getMetaData

    while (resultSet.next()) {
      val rowDate = new JSONObject()
      for (i <- 1 to metaData.getColumnCount) {
        rowDate.put(metaData.getColumnName(i), resultSet.getObject(i))
      }
      resultList += rowDate
    }

    resultSet.close()
    statement.close()
    conn.close()

    resultList.toList

  }


}
