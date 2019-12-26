package com.itheima.syncdb.bean

import com.alibaba.fastjson.JSON

/**
  * 封装binlog消息的样例类
  * @param emptyCount 操作次数
  * @param logFileName  binlog文件名
  * @param dbName 数据库名
  * @param logFileOffset  binlog文件中的偏移量
  * @param eventType  操作方式（INSERT/UPDATE/DELETE）
  * @param columnValueList  列值列表
  * @param tableName  表名
  * @param timestamp  时间戳
  */
case class Canal (var emptyCount:Long,
                  var logFileName:String,
                  var dbName:String,
                  var logFileOffset:Long,
                  var eventType:String,
                  var columnValueList:String,
                  var tableName:String,
                  var timestamp:Long)

object Canal {
  def apply(json:String): Canal = {
    val jsonObject = JSON.parseObject(json)
    new Canal(
      jsonObject.getLong("emptyCount"),
      jsonObject.getString("logFileName"),
      jsonObject.getString("dbName"),
      jsonObject.getLong("logFileOffset"),
      jsonObject.getString("eventType"),
      jsonObject.getString("columnValueList"),
      jsonObject.getString("tableName"),
      jsonObject.getLong("timestamp")
    )
  }

  def main(args: Array[String]): Unit = {
    val json = "{\"emptyCount\":2,\"logFileName\":\"mysql-bin.000002\",\"dbName\":\"pyg\",\"logFileOffset\":344,\"eventType\":\"INSERT\",\"columnValueList\":[{\"columnName\":\"categoryId\",\"columnValue\":\"1\",\"isValid\":true},{\"columnName\":\"categoryName\",\"columnValue\":\"输入初始密码，此时不能做任何事情，因为M\",\"isValid\":true},{\"columnName\":\"categoryGrade\",\"columnValue\":\"3\",\"isValid\":true}],\"tableName\":\"category\",\"timestamp\":1571970560000}"
    println(Canal(json))
  }
}