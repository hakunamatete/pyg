package com.itheima.batch_process.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.beans.BeanProperty

class HBaseTableInputFormat(var tableName:String) extends TableInputFormat[org.apache.flink.api.java.tuple.Tuple2[String, String]]{


  var conn: Connection = _

  override def getScanner: Scan = {
    scan = new Scan()
    scan
  }

  override def getTableName: String = tableName

  override def mapResultToTuple(result: Result): org.apache.flink.api.java.tuple.Tuple2[String, String] = {
    val rowkey = Bytes.toString(result.getRow)

    // 获取列单元格

    val cellArray = result.rawCells()
    val jsonObject = new JSONObject

    for(i <- 0 until cellArray.size) {
      val columnFamilyName = Bytes.toString(CellUtil.cloneFamily(cellArray(i)))
      val columnName = Bytes.toString(CellUtil.cloneQualifier(cellArray(i)))
      val value = Bytes.toString(CellUtil.cloneValue(cellArray(i)))

      jsonObject.put(columnName, value)
    }


    new org.apache.flink.api.java.tuple.Tuple2[String, String](rowkey, JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect))
  }

  override def close(): Unit = {
    if(table != null) table.close()
    if(conn != null) conn.close()
  }
}
