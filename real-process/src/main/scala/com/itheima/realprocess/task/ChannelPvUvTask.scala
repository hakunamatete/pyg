package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 小时维度PV/UV业务开发
  * PV(访问量)：即Page View，页面刷新一次算一次。
  * UV(独立访客)：即Unique Visitor，指定时间内相同的客户端只被计算一次
  * 频道ID     时间       PV(访问量)   UV(独立访客)
  * 频道1     2017010116  1230        350
  * 频道2     2017010117  1251        330
  * 频道3     2017010118  5512        610
  */
object ChannelPvUvTask {

  // 1. 添加一个`ChannelPvUv`样例类，它封装要统计的四个业务字段：频道ID（channelID）、年月日时、PV、UV
  case class ChannelPvUv(var channelID:String,
                         var yearMonthDayHour:String,
                         var pv:Long,
                         var uv:Long)

  def processHourDim(clicklogWideDataStream:DataStream[ClickLogWide]) = {

    // 3. 使用map算子，将`ClickLog`对象转换为`ChannelPvUv`
    val channelPvUvDataStream = clicklogWideDataStream.flatMap {
      clicklog =>
        List(
          ChannelPvUv(clicklog.channelID, clicklog.yearMonthDayHour, clicklog.count, clicklog.isHourNew),
          ChannelPvUv(clicklog.channelID, clicklog.yearMonthDay, clicklog.count, clicklog.isDayNew),
          ChannelPvUv(clicklog.channelID, clicklog.yearMonth, clicklog.count, clicklog.isMonthNew)
        )
    }
    // 4. 按照`频道ID`、`年月日时`进行分流val groupedDataStream: KeyedStream[ChannelPvUv, String] = channelPvUvDataStream.keyBy {
    val groupedDataStream: KeyedStream[ChannelPvUv, String] = channelPvUvDataStream.keyBy {
      channelPvUv => channelPvUv.channelID + channelPvUv.yearMonthDayHour
    }

    // 5. 划分时间窗口（3秒一个窗口）
    val windowedStream: WindowedStream[ChannelPvUv, String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    // 6. 执行reduce合并计算
    val reduceDataStream: DataStream[ChannelPvUv] = windowedStream.reduce {
      (pvuv1, pvuv2) =>
        ChannelPvUv(pvuv2.channelID, pvuv2.yearMonthDayHour, pvuv1.pv + pvuv2.pv, pvuv1.uv + pvuv2.uv)
    }
    reduceDataStream.print()

    // 7. 将合并后的数据下沉到hbase
    reduceDataStream.addSink{
      pvuv =>
        val tableName = "channel_pvuv"
        val cfName = "info"
        val channelIdColName = "channelID"
        val yearMonthDayHourColName = "yearMonthDayHour"
        val pvColName = "pv"
        val uvColName = "uv"
        val rowkey = pvuv.channelID + ":" + pvuv.yearMonthDayHour

        var totalPv = 0L
        var totalUv = 0L

        // - 判断hbase中是否已经存在结果记录
        val pvValue: String = HbaseUtil.getData(tableName, rowkey, cfName, pvColName)
        val uvValue: String = HbaseUtil.getData(tableName, rowkey, cfName, uvColName)

        // - 若存在，则获取后进行累加
        if(!StringUtils.isBlank(pvValue)) {
          totalPv = pvValue.toLong + pvuv.pv
        }
        // - 若不存在，则直接写入
      else {
        totalPv = pvuv.pv
      }
      // - 若存在，则获取后进行累加
      if(!StringUtils.isBlank(uvValue)) {
        totalUv = uvValue.toLong + pvuv.uv
      }
      // - 若不存在，则直接写入
      else {
        totalUv = pvuv.uv
      }

      HbaseUtil.putMapData(tableName, rowkey, cfName, Map(
        channelIdColName -> pvuv.channelID,
        yearMonthDayHourColName -> pvuv.yearMonthDayHour,
        pvColName -> totalPv.toString,
        uvColName -> totalUv.toString
      ))
    }
  }
}
