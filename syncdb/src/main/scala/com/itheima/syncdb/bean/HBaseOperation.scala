package com.itheima.syncdb.bean

/**
  * HBase操作样例类
  *
  * @param opType 操作类型
  * @param tableName 表名
  * @param cfName 列蔟名
  * @param rowkey rowkey唯一主键
  * @param colName 列名
  * @param colValue 列值
  */
case class HBaseOperation (var opType: String,
                           var tableName: String,
                           var cfName: String,
                           var rowkey: String,
                           var colName: String,
                           var colValue: String)
