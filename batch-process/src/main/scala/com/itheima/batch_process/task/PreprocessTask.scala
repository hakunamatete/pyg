package com.itheima.batch_process.task

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object PreprocessTask {

  /**
    * 对原有的订单数据进行拓宽处理
    * 主要拓宽时间字段
    *
    * @param orderRecordDataSet
    */
  def process(orderRecordDataSet: DataSet[OrderRecord]) = {
    // 将原有的订单数据，扩展一些字段用于后续的分析
    orderRecordDataSet.map {
      order =>
        val originalDateTime = order.payTime
        val yearMonthDay = dateFormat(originalDateTime, "yyyyMMdd")
        val yearMonth = dateFormat(originalDateTime, "yyyyMM")
        val year = dateFormat(originalDateTime, "yyyy")

        OrderRecordWide(order.benefitAmount,
          order.orderAmount,
          order.payAmount,
          order.activityNum,
          order.createTime,
          order.merchantId,
          order.orderId,
          order.payTime,
          order.payMethod,
          order.voucherAmount,
          order.commodityId,
          order.userId,
          yearMonthDay,
          yearMonth,
          year)
    }
  }

  def dateFormat(originalDateTime: String, format: String) = {
    // 先将原始的日期时间转换为时间戳
    val dateFormater1 = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    // 通过parse解析日期字符串，再通过getTime获取到对应的时间戳
    val timestamp = dateFormater1.parse(originalDateTime).getTime

    // 再将时间戳转换为指定日期的格式
    val dateFormat2 = FastDateFormat.getInstance(format)
    dateFormat2.format(timestamp)
  }

  def main(args: Array[String]): Unit = {
    println(dateFormat("2018-08-13 00:00:06", "yyyy"))
    println(dateFormat("2018-08-13 00:00:06", "yyyyMM"))
    println(dateFormat("2018-08-13 00:00:06", "yyyyMMdd"))
  }
}
