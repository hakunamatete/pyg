package com.itheima.syncdb.task

import com.alibaba.fastjson.JSON
import com.itheima.syncdb.bean.{Canal, HBaseOperation}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

object PreprocessTask {

  /**
    * 封装列名、列值、是否有效
    *
    * @param colName 列名
    * @param colValue 列值
    * @param isValid INSERT->true，UPDATE->有一部分是true，有一部分是false, DELETE->FALSE
    */
  case class ColNameValuePair(colName:String, colValue:String, isValid:Boolean)

  def process(watermaerkDataStream:DataStream[Canal]) = {
    // 将canal消息 -> HBaseOperation样例类
    watermaerkDataStream.flatMap{
      canal =>
        // 根据eventType分别处理HBaseOperation列表
        // rowkey就是第一个列的值
        val opType = canal.eventType
        // 生成的表名为mysql.数据库名.表名
        val tableName = s"mysql.${canal.dbName}.${canal.tableName}"
        val cfName = "info"

        // JSON字符串canal.columnValueList
        // 需要进一步处理，将这个JSON字符串转换为scala样例类列表
        // 获取列值列表
        val colNameValuePairList: List[ColNameValuePair] = parseColNameValueListJson(canal.columnValueList)

        // 获取mysql中的主键，获取第一列的值
        val rowkey: String = colNameValuePairList(0).colValue

        // INSERT操作 -> 将所有列值转换为HBaseOperation

        canal.eventType match {
          case "INSERT" =>
            // 如果是INSERT操作，将每一个列值对转换为一个HBaseOperation
            colNameValuePairList.map{
              colValue =>
                HBaseOperation(opType, tableName, cfName, rowkey, colValue.colName, colValue.colValue)
            }
          case "UPDATE" =>
            // UPDATE操作 -> 过滤出来isValid字段为true的列，再转换为HBaseOperation
            colNameValuePairList.filter(_.isValid).map {
              colValue =>
                HBaseOperation(opType, tableName, cfName, rowkey, colValue.colName, colValue.colValue)
            }
          case "DELETE" =>
            // DELETE操作 -> 只生成一条DELETE的HBaseOperation的List
            List(HBaseOperation(opType, tableName, cfName, rowkey, "", ""))
        }
    }
  }

  // 将这个JSON字符串转换为scala样例类列表
  def parseColNameValueListJson(columnValueList:String) = {
    // 使用FastJSON将字符串解析为JSONArray
    val jsonArray = JSON.parseArray(columnValueList)

    // 创建一个可变的列表，用来添加列名列值对
    val colNameValuePairList = scala.collection.mutable.ListBuffer[ColNameValuePair]()

    // 遍历JSONArray,使用until
    for(i <- 0 until jsonArray.size()) {
      // 解析JSON为JSONObject
      // {"isValid":false,"columnValue":"1","columnName":"commodityId"}
      val jsonObject = jsonArray.getJSONObject(i)
      // 将解析出来的字段封装在样例类中
      // 添加到可变列表
      colNameValuePairList +=
        ColNameValuePair(jsonObject.getString("columnName"),
          jsonObject.getString("columnValue"),
          jsonObject.getBoolean("isValid")
        )
    }
    colNameValuePairList.toList
  }

  def main(args: Array[String]): Unit = {
    val json = "[{\"isValid\":false,\"columnValue\":\"1\",\"columnName\":\"commodityId\"},{\"isValid\":false,\"columnValue\":\"耐克\",\"columnName\":\"commodityName\"},{\"isValid\":false,\"columnValue\":\"1\",\"columnName\":\"commodityTypeId\"},{\"isValid\":true,\"columnValue\":\"20.0\",\"columnName\":\"originalPrice\"},{\"isValid\":false,\"columnValue\":\"820.0\",\"columnName\":\"activityPrice\"}]"
    println(parseColNameValueListJson(json))
  }
}
