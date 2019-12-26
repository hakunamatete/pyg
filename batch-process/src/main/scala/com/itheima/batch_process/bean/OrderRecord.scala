package com.itheima.batch_process.bean

import com.alibaba.fastjson.JSON

case class OrderRecord(benefitAmount:Double,  // 红包金额
                       orderAmount:Double,    // 订单金额
                       payAmount:Double,      // 支付金额
                       activityNum:String,    // 获取ID
                       createTime:String,     // 订单创建时间
                       merchantId:String,     // 商家ID
                       orderId:String,        // 订单ID
                       payTime:String,        // 支付时间
                       payMethod:String,      // 支付方式
                       voucherAmount:String,  // 优惠券金额
                       commodityId:String,    // 产品ID
                       userId:String)        // 用户ID

object OrderRecord {
  def apply(json:String): OrderRecord = {
    val jsonObject = JSON.parseObject(json)

    OrderRecord(
      jsonObject.getDouble("benefitAmount"),
      jsonObject.getDouble("orderAmount"),
      jsonObject.getDouble("payAmount"),
      jsonObject.getString("activityNum"),
      jsonObject.getString("createTime"),
      jsonObject.getString("merchantId"),
      jsonObject.getString("orderId"),
      jsonObject.getString("payTime"),
      jsonObject.getString("payMethod"),
      jsonObject.getString("voucherAmount"),
      jsonObject.getString("commodityId"),
      jsonObject.getString("userId"))
  }

  def main(args: Array[String]): Unit = {
    // 测试apply方法是否能够封装JSON数据
    val json = "{\"benefitAmount\":\"20.0\",\"orderAmount\":\"300.0\",\"payAmount\":\"457.0\",\"activityNum\":\"0\",\"createTime\":\"2018-08-13 00:00:06\",\"merchantId\":\"1\",\"orderId\":\"99\",\"payTime\":\"2018-08-13 00:00:06\",\"payMethod\":\"1\",\"voucherAmount\":\"20.0\",\"commodityId\":\"1101\",\"userId\":\"4\"}"
    println(OrderRecord(json))
  }
}