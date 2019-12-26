package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
  * 预处理：地址、时间字段拓宽
  */
object PreprocessTask {

  // 包装isNew字段的样例类
  case class IsNewWrapper(isNew:Int, isHourNew:Int, isDayNew:Int, isMonthNew:Int)

  def process(watermakerData:DataStream[Message]):DataStream[ClickLogWide]={
    // 使用 map 算子进行数据转换
    watermakerData.map{
      msg =>
        val count = msg.count
        val timestamp = msg.timestamp
        // 国家省份城市（拼接）（address)
        val address = msg.clicklog.country + msg.clicklog.province + msg.clicklog.city
        // 年月（yearMonth)
        val yearMonth = timestamp2Date(timestamp,"yyyyMM")
        // 年月日（yearMonthDay)
        val yearMonthDay = timestamp2Date(timestamp,"yyyyMMdd")
        // 年月日时（yearMonthDayHour)
        val yearMonthDayHour = timestamp2Date(timestamp,"yyyyMMddHH")
        // 获取用户是否为新用户
        val isNewWrapper: IsNewWrapper = analysisIsNew(msg)

        // 构建ClickLogWide样例类
        ClickLogWide(msg.clicklog.browserType,
          msg.clicklog.categoryID,
          msg.clicklog.channelID,
          msg.clicklog.city,
          msg.clicklog.country,
          msg.clicklog.entryTime,
          msg.clicklog.leaveTime,
          msg.clicklog.network,
          msg.clicklog.produceID,
          msg.clicklog.province,
          msg.clicklog.source,
          msg.clicklog.userID,
          count,
          timestamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour,
          isNewWrapper.isNew,
          isNewWrapper.isHourNew,
          isNewWrapper.isDayNew,
          isNewWrapper.isMonthNew
        )

    }

    // 在 App 中调用预处理任务的 process 方法，并打印测试
  }

  /**
    * 將时间戳转换为指定日期格式
    * @param timestamp 时间戳
    * @param foramt 日期格式
    */
  def timestamp2Date(timestamp:Long,foramt:String): String={
    val formater = FastDateFormat.getInstance(foramt)
    formater.format(timestamp)
  }

  /**
    * 判断用户是否是新用户、小时/天/月是否为新用户
    * @param msg
    */
  def analysisIsNew(msg:Message) = {

    //先把要拓宽的字段isNew、isHourNew、isDayNew、isMonthNew都创建出来，初始化为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0

    // 封装操作hbase需要的字段
    val tableName = "user_history"
    val rowkey = msg.clicklog.userID + ":" + msg.clicklog.channelID
    val cfName = "info"
    val userIdColName = "userId"        // 用户ID
    val channelIdColName = "channelId"  // 频道ID
    val lastVisitedTimeColName = "lastVisitedTime"  // 该用户最后一次访问的时间

    //从hbase查询rowkey为userid:channlid查询user_history中userid列的数据
    val userIdInHBase = HbaseUtil.getData(tableName, rowkey, cfName, userIdColName)

    //判断userid列数据是否为空
    if(StringUtils.isBlank(userIdInHBase)) {
      //如果为空
      //设置isNew字段为1，表示是新用户，
      //将其他的isHourNew、isDayNew、isMonthNew字段都设置为1
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      //将该用户数据添加到user_history表中
      HbaseUtil.putMapData(tableName, rowkey, cfName, Map(
        channelIdColName -> msg.clicklog.channelID,
        userIdColName -> msg.clicklog.userID,
        lastVisitedTimeColName -> msg.timestamp
      ))
    }
    else {
      //如果不为空
      //设置isNew字段为0，表示是老用户
      isNew = 0

      // 从user_history表中获取lastVisitedTime字段
      val lastVisitTime = HbaseUtil.getData(tableName, rowkey, cfName, lastVisitedTimeColName).toLong

      // 将lastVisitedTime字段格式化为年月日时格式
      val yearMonthDayHourInHBase = timestamp2Date(lastVisitTime, "yyyyMMddHH")
      val yearMonthDayInHBase = timestamp2Date(lastVisitTime, "yyyyMMdd")
      val yearMonthInHBase = timestamp2Date(lastVisitTime, "yyyyMM")

      // 和当前点击流日志的年/月/日/时字段比较
      val yearMonthDayHour = timestamp2Date(msg.timestamp, "yyyyMMddHH")
      val yearMonthDay = timestamp2Date(msg.timestamp, "yyyyMMdd")
      val yearMonth = timestamp2Date(msg.timestamp, "yyyyMM")

      // 如果当前时间字段 > lastVisited，则isHourNew为1
      if(yearMonthDayHour > yearMonthDayHourInHBase) {
        isHourNew = 1
      }
      else {
        // 否则isHourNew为0
        isHourNew = 0
      }

      // 计算isDayNew
      if(yearMonthDay > yearMonthDayInHBase) {
        isDayNew = 1
      }
      else {
        isDayNew = 0
      }

      // 计算isMonthNew
      if(yearMonth > yearMonthInHBase) {
        isMonthNew = 1
      }
      else {
        isMonthNew = 0
      }

      //更新user_history表中的lastVisitedTime列
      HbaseUtil.putData(tableName, rowkey, cfName, lastVisitedTimeColName, msg.timestamp)
    }

    IsNewWrapper(isNew, isHourNew, isDayNew, isMonthNew)
  }
}
