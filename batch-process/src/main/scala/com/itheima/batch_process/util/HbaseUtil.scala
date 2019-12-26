package com.itheima.batch_process.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

object HbaseUtil {

  //  使用 HBaseConfiguration.create 获取配置对象 Configuration ，该配置对象会自动加载hbase-site.xml
  private val configuration: Configuration = HBaseConfiguration.create()

  //  使用 ConnectionFactory.createConnection 获取hbase连接
  private val connection: Connection = ConnectionFactory.createConnection(configuration)

  //  使用 Connection.getAdmin 获取与master的连接
  private val admin: Admin = connection.getAdmin

  /**
    * 获取hbase表
    * @param tableNameStr 表名称
    * @param cfName 列簇
    */
  def getTable(tableNameStr:String,
               cfName:String): Table={
    val tableName = TableName.valueOf(tableNameStr)
  // 检查表是否存在
    if(!admin.tableExists(tableName)){
      // 构建HTableDescriptor
      val tableDesc = new HTableDescriptor(tableName)
      // 构建一个列簇描述器
      val cfDesc = new HColumnDescriptor(cfName)
      // 给表指定一个列簇
      tableDesc.addFamily(cfDesc)
      // 若不存在，则创建表
      admin.createTable(tableDesc)
    }
    connection.getTable(tableName)
  }

  /**
    * 存放一条数据到HBase中
    * @param tableNameStr 表名
    * @param rowkey   rowkey
    * @param cfName   列簇
    * @param colName  列名
    * @param colValue 列值
    */
  def putData(tableNameStr:String,
              rowkey:String,
              cfName:String,
              colName:String,
              colValue:Any):Unit={

    // 调用getTable获取表
    val table = getTable(tableNameStr,cfName)
    try{
      // 将字符串类型转换成字节数组
      val rowKeyBytes = Bytes.toBytes(rowkey)
      val cfNameBytes = Bytes.toBytes(cfName)
      val colNameBytes = Bytes.toBytes(colName)
      val colValueBytes = Bytes.toBytes(colValue.toString)
      // 构建 Put 对象
      // 传入一个rowkey，要求传入的是一个字节数组
      val put = new Put(rowKeyBytes)
      put.addColumn(cfNameBytes,colNameBytes,colValueBytes)
      // 添加列、列值
      // 对table执行put操作
      table.put(put)
    }catch{
      case ex:Exception => ex.printStackTrace()
    }finally {
      table.close
    }
  }

  def putMapData(tableNameStr:String,
                 rowkey:String,
                 cfName:String,
                 map:Map[String,Any]):Unit={
    // 调用getTable获取表
    val table = getTable(tableNameStr,cfName)
    try{
      // 将字符串类型转换成字节数组
      val rowKeyBytes = Bytes.toBytes(rowkey)
      val cfNameBytes = Bytes.toBytes(cfName)
      // 构建 Put 对象
      // 传入一个rowkey，要求传入的是一个字节数组
      val put = new Put(rowKeyBytes)
      for((colName,colValue) <- map){
        val colNameBytes = Bytes.toBytes(colName)
        val colValueBytes = Bytes.toBytes(colValue.toString)
        put.addColumn(cfNameBytes,colNameBytes,colValueBytes)
      }
      // 添加列、列值
      // 对table执行put操作
      table.put(put)
    }catch{
      case ex:Exception => ex.printStackTrace()
    }finally {
      // 关闭表
      table.close
    }
  }

  /**
    * 获取数据
    *
    * @param tableNameStr 表名
    * @param rowkey rowkey
    * @param cfName 列蔟名
    * @param colName 列名
    * @return
    */
  def getData(tableNameStr:String,
              rowkey:String,
              cfName:String,
              colName:String):String= {
    // 获取表
    val table = getTable(tableNameStr, cfName)

    try {
      // 将字符串类型转换成字节数组
      val rowKeyBytes = Bytes.toBytes(rowkey)
      val cfNameBytes = Bytes.toBytes(cfName)
      val colNameBytes = Bytes.toBytes(colName)

      // 构建Get对象
      val get = new Get(rowKeyBytes)
      get.addFamily(cfNameBytes)

      // 对table执行get操作，获取result
      val result = table.get(get)

      if (result != null && result.containsColumn(cfNameBytes, colNameBytes)) {
        // 使用Result.getValue获取列蔟列对应的值
        val bytes = result.getValue(cfNameBytes, colNameBytes)
        Bytes.toString(bytes)
      }else{
        ""
      }
    }
    catch {
      // 捕获异常
      case ex:Exception => ex.printStackTrace()
        ""
    }
    finally {
      // 关闭表
      table.close()
    }
  }

  /**
    * 删除hbase的一条数据
    *
    * @param tableNameStr 表名
    * @param rowkey rowkey
    * @param cfName 列蔟名
    */
  def deleteData(tableNameStr:String,
                 rowkey:String,
                 cfName:String) = {
    // 获取表
    val table = getTable(tableNameStr, cfName)

    try {
      // 将字符串转换为字节数组
      val rowKeyBytes = Bytes.toBytes(rowkey)
      val cfNameBytes = Bytes.toBytes(cfName)

      val delete = new Delete(rowKeyBytes)
      delete.addFamily(cfNameBytes)

      table.delete(delete)
    }
    catch {
      case ex:Exception => ex.printStackTrace()
    }
    finally {
      table.close()
    }
  }

  def main(args: Array[String]): Unit = {
//    putData("test","123","info","t1","this is values")
    putMapData("test","123","info",Map(
      "order" -> 1,
      "userId" -> "zhangsan",
      "money" -> 123.12,
      "createTime" -> "2019-09-10 10:00:00"
    ))
    println(getData("test","123","info","order"))
    println(getData("test","123","info","userId"))
    println(getData("test","123","info","money"))
    println(getData("test","123","info","createTime"))
    deleteData("test", "123", "info")
  }
}
