package com.itheima.batch_process.task

import com.itheima.batch_process.bean.OrderRecordWide
import com.itheima.batch_process.util.HbaseUtil
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
  * 按照不同的时间维度来统计不同支付方式的订单金额、订单数量
  */
object PaymethodTask {

  // 设计一个样例类来保存统计的数据
  case class PaymethodMoneyCount(paymethod:String, date:String, money:Double, count:Long)


  def process(orderRecordWide:DataSet[OrderRecordWide]) = {
    // Money => Mny
    // Count => cnt
    // Message => msg
    val paymethodMnyCnt = orderRecordWide.flatMap{
      order =>
        // 因为当前要统计不同时间维度的数据
        // 使用flatMap来将一条数据转换为不同维度的多条数据
        List(
          PaymethodMoneyCount(order.payMethod, order.yearMonthDay, order.payAmount, 1), // 天维度
          PaymethodMoneyCount(order.payMethod, order.yearMonth, order.payAmount, 1), // 月维度
          PaymethodMoneyCount(order.payMethod, order.year, order.payAmount, 1)   // 年维度
        )
    }

    // 按照指定的字段进行分组
    val groupedDataSet = paymethodMnyCnt.groupBy{
      pay =>
        pay.paymethod + pay.date
    }

    // 进行聚合计算
    val reducedDataSet = groupedDataSet.reduce{
      (p1, p2) =>
        PaymethodMoneyCount(p2.paymethod, p2.date, p1.money + p2.money, p1.count + p2.count)
    }

    // 打印测试
    reducedDataSet.print()

    // 将结果数据保存到hbase中
    reducedDataSet.collect().foreach{
      pay =>
        // 构建hbase所必须的参数
        val tableName = "analysis_payment"
        val rowkey = pay.paymethod + ":" + pay.date
        val cfName = "info"
        val paymethodColName = "paymethod"
        val dateColName = "date"
        val totalMoneyColName = "totalMoney"
        val totalCountColName = "totalCount"

        // 将Flink计算后的结果写入到hbase
        HbaseUtil.putMapData(tableName, rowkey, cfName, Map(
          paymethodColName -> pay.paymethod,
          dateColName -> pay.date,
          totalMoneyColName -> pay.money,
          totalCountColName -> pay.count
        ))
    }

  }
}
