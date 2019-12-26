package com.itheima.realprocess.bean

/**
  * 保存点击流拓宽后的字段
  *
  * 用户访问的次数（count)
  * 用户访问的时间（timestamp)
  * 国家省份城市（拼接）（address)
  * 年月（yearMonth)
  * 年月日（yearMonthDay)
  * 年月日时（yearMonthDayHour)
  * 是否为访问某个频道的新用户（isNew)——0表示否，1表示是
  * 在某一小时内是否为某个频道的新用户（isHourNew)——0表示否
  * 在某一天是否为某个频道的新用户（isDayNew)——0表示否，1表示是，
  * 在某一个月是否为某个频道的新用户（isMonthNew)——0表示否，1表示是
  */
case class ClickLogWide(browserType: String, // 浏览器类型
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
                        userID: String,
                        count:Long,
                        timestamp:Long,
                        address:String,
                        yearMonth:String,
                        yearMonthDay:String,
                        yearMonthDayHour:String,
                        isNew:Int,
                        isHourNew:Int,
                        isDayNew:Int,
                        isMonthNew:Int)

