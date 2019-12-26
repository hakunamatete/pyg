package com.itheima.realprocess.bean

import com.alibaba.fastjson.JSON

/**
  * 使用ClickLog样例类来封装点击流日志
  */
case class ClickLog(browserType: String, // 浏览器类型
                    categoryID: String, // 产品分类ID
                    channelID: String, // 频道ID
                    city: String, // 城市
                    country: String, // 国家
                    entryTime: String, // 进入时间
                    leaveTime: String, // 离开时间
                    network: String, // 运营商
                    produceID: String, // 产品ID
                    province: String, // 省份
                    source: String, // 来源
                    userID: String) // 用户ID

object ClickLog {
  //{"browserType":"360浏览器","categoryID":13,"channelID":19,"city":"ShiJiaZhuang",
  // "country":"china","entryTime":1544605260000,"leaveTime":1544634060000,
  // "network":"移动","produceID":11,"province":"Beijing","source":"必应跳转","userID":11}
  def apply(json: String): ClickLog = {
    //使用FastJSON的JSON.parseObject方法将JSON字符串构建一个ClickLog实例对象
    val jsonObject = JSON.parseObject(json)

    ClickLog(
      jsonObject.getString("browserType"),
      jsonObject.getString("categoryID"),
      jsonObject.getString("channelID"),
      jsonObject.getString("city"),
      jsonObject.getString("country"),
      jsonObject.getString("entryTime"),
      jsonObject.getString("leaveTime"),
      jsonObject.getString("network"),
      jsonObject.getString("produceID"),
      jsonObject.getString("province"),
      jsonObject.getString("source"),
      jsonObject.getString("userID")
    )
  }

  //在样例类中编写一个main方法，传入一些JSON字符串测试是否能够正确解析
  def main(args: Array[String]): Unit = {
    val json = "{\"browserType\":\"360浏览器\",\"categoryID\":13,\"channelID\":19,\"city\":\"ShiJiaZhuang\",\"country\":\"china\",\"entryTime\":1544605260000,\"leaveTime\":1544634060000,\"network\":\"移动\",\"produceID\":11,\"province\":\"Beijing\",\"source\":\"必应跳转\",\"userID\":11}"
    println(ClickLog(json))
  }
}