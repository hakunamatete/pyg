package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 需要分别统计不同浏览器（或者客户端）的占比
  * 频道ID  浏览器       时间          PV      UV      新用户 老用户
  * 频道1   360浏览器  201809        1000    300       0       300
  * 频道1   IE        20180910      123     1         0       1
  * 频道1   Chrome    2018091010    55      2         2       0
  */
object ChannelBrowserTask {
  // 2. 添加一个`ChannelNetwork`样例类，它封装要统计的四个业务字段：频道ID（channelID）、运营商（network）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
  case class ChannelBrowser(var channelID: String,
                            var browser: String,
                            var date: String,
                            var pv: Long,
                            var uv: Long,
                            var newCount: Long,
                            var oldCount: Long)
  // 3. 在`ChannelNetworkTask`中编写一个`process`方法，接收预处理后的`DataStream`
  def process(clicklogWideDataStream: DataStream[ClickLogWide]) = {
    // 4. 使用`flatMap`算子，将`ClickLog`对象转换为三个不同时间维度`ChannelBrowser`
    val channelBrowserDataStream = clicklogWideDataStream.flatMap{
      clicklog =>
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelBrowser(clicklog.channelID,clicklog.browserType,clicklog.yearMonthDayHour,clicklog.count,clicklog.isHourNew,clicklog.isNew,isOld(clicklog.isNew,clicklog.isHourNew)),// 小时维度
          ChannelBrowser(clicklog.channelID,clicklog.browserType,clicklog.yearMonthDay,clicklog.count,clicklog.isDayNew,clicklog.isNew,isOld(clicklog.isNew,clicklog.isDayNew)),// 天维度
          ChannelBrowser(clicklog.channelID,clicklog.browserType,clicklog.yearMonth,clicklog.count,clicklog.isMonthNew,clicklog.isNew,isOld(clicklog.isNew,clicklog.isMonthNew)) // 月维度
        )
    }
    // 5. 按照`频道ID`、`时间`、`浏览器`进行分组
    val browserDataStream = channelBrowserDataStream.keyBy {
      browser =>
        browser.channelID + browser.date + browser.browser
    }
    // 6. 划分时间窗口（3秒一个窗口）
    val windowDataStream: WindowedStream[ChannelBrowser, String, TimeWindow] = browserDataStream.timeWindow(Time.seconds(3))

    // 7. 执行reduce合并计算
    val reduceDataStream = windowDataStream.reduce{
      (browser1,browser2) =>
        ChannelBrowser(browser2.channelID,browser2.browser,browser2.date,browser2.pv + browser1.pv, browser2.uv + browser1.uv,browser2.newCount + browser1.newCount,browser2.oldCount + browser1.oldCount)
    }
    // 8. 打印测试
    reduceDataStream.print()

    //9. 将合并后的数据下沉到hbase
    reduceDataStream.addSink{
      browser =>
        //- 准备hbase的表名、列蔟名、rowkey名、列名
        val tableName = "channel_browser"
        val rowkey = s"${browser.channelID}:${browser.date}:${browser.browser}"
        val cfName = "info"

        // 频道ID（channelID）、浏览器（browser）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
        val channelIdColName = "channelID"
        val browserColName = "browser"
        val dateColName = "date"
        val pvColName = "pv"
        val uvColName = "uv"
        val newCountColName = "newCount"
        val oldCountColName = "oldCount"

        // 获取HBase中的历史指标数据
        val pvInHBase= HbaseUtil.getData(tableName,rowkey,cfName,pvColName)
        val uvInHBase= HbaseUtil.getData(tableName,rowkey,cfName,uvColName)
        val newInHBase= HbaseUtil.getData(tableName,rowkey,cfName,newCountColName)
        val oldInHBase= HbaseUtil.getData(tableName,rowkey,cfName,oldCountColName)

        var totalPv = 0L
        var totalUv = 0L
        var totalNewCount = 0L
        var totalOldCount = 0L

        //- 判断hbase中是否已经存在结果记录
        //- 若存在，则获取后进行累加
        if(StringUtils.isNotBlank(pvInHBase)) {
          totalPv = pvInHBase.toLong + browser.pv
        } else {
          totalPv = browser.pv
        }
        if(StringUtils.isNotBlank(uvInHBase)) {
          totalUv = uvInHBase.toLong + browser.uv
        } else {
          totalUv = browser.uv
        }
        if(StringUtils.isNotBlank(newInHBase)) {
          totalNewCount = newInHBase.toLong + browser.newCount
        } else {
          totalNewCount = browser.newCount
        }
        if(StringUtils.isNotBlank(oldInHBase)) {
          totalOldCount = oldInHBase.toLong + browser.oldCount
        } else {
          totalOldCount = browser.oldCount
        }
        HbaseUtil.putMapData(tableName,rowkey,cfName,Map(
          // 频道ID（channelID）、地域（area）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
          channelIdColName -> browser.channelID,
          browserColName -> browser.browser,
          dateColName -> browser.date,
          pvColName -> totalPv.toString,
          uvColName -> totalUv.toString,
          newCountColName -> totalNewCount.toString,
          oldCountColName -> totalOldCount.toString
        ))
    }
  }
}