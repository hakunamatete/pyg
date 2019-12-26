package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 用户新鲜度即分析网站每小时、每天、每月活跃的新老用户占比
  *统计分析要得到的数据如下：
  * 频道ID 时间 新用户 老用户
  * 频道1 201703 512 144
  * 频道1 20170318 411 4123
  * 频道1 2017031810 342 4412
  */
object ChannelFreshnessTask {

  // 1. 添加一个`ChannelFreshness`样例类，它封装要统计的四个业务字段：频道ID（channelID）、日期（date）、新用户（newCount）、老用户（oldCount）
  case class ChannelFreshness(var channelID:String,
                              var date:String,
                              var newCount:Long,
                              var oldCount:Long)

  // 2. 在`ChannelFreshnessTask`中编写一个`process`方法，接收预处理后的`DataStream`
  def process(clicklogWideDataStream:DataStream[ClickLogWide]) = {
    // 3. 使用flatMap算子，将`ClickLog`对象转换为`ChannelFreshness`
    val channelFreshnessDataStream: DataStream[ChannelFreshness] = clicklogWideDataStream.flatMap {
      clicklog =>
        val isOld = (isNew: Int, isDateNew:Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelFreshness(clicklog.channelID, clicklog.yearMonthDayHour, clicklog.isNew, isOld(clicklog.isNew,clicklog.isHourNew)),
          ChannelFreshness(clicklog.channelID, clicklog.yearMonthDay, clicklog.isNew, isOld(clicklog.isDayNew,clicklog.isDayNew)),
          ChannelFreshness(clicklog.channelID, clicklog.yearMonth, clicklog.isNew, isOld(clicklog.isMonthNew,clicklog.isMonthNew))
        )
    }

    // 4. 按照`频道ID`、`日期`进行分流
    val groupedDateStream = channelFreshnessDataStream.keyBy {
      freshness =>
        freshness.channelID + freshness.date
    }
    // 5. 划分时间窗口（3秒一个窗口）
    val windowStream = groupedDateStream.timeWindow(Time.seconds(3))

    // 6. 执行reduce合并计算
    val reduceDataStream: DataStream[ChannelFreshness] = windowStream.reduce {
      (freshness1, freshness2) =>
        ChannelFreshness(freshness2.channelID, freshness2.date, freshness1.newCount + freshness2.newCount, freshness1.oldCount + freshness2.oldCount)
    }
    // 打印测试
    reduceDataStream.print()
    // 7. 将合并后的数据下沉到hbase
    reduceDataStream.addSink{
      freshness =>
        val tableName = "channel_freshness"
        val rowkey = freshness.channelID + ":" + freshness.date
        val cfName = "info"
        // 频道ID（channelID）、日期（date）、新用户（newCount）、老用户（oldCount）
        val channelIdColName = "channelID"
        val dateColName = "date"
        val newCountColName = "newCount"
        val oldCountColName = "oldCount"
        // - 判断hbase中是否已经存在结果记录
        val newCountOldCountMap = HbaseUtil.getData(tableName, rowkey, cfName, newCountColName)
        val oldCountOldCountMap = HbaseUtil.getData(tableName, rowkey, cfName, oldCountColName)

        var totalNewCount = 0L
        var totalOldCount = 0L

        // - 若存在，则获取后进行累加
        if(StringUtils.isNotBlank(newCountOldCountMap)){
          totalNewCount = newCountOldCountMap.toLong + freshness.newCount
        }else{
          // - 若不存在，则直接写入
          totalNewCount = freshness.newCount
        }

        // - 若存在，则获取后进行累加
        if(StringUtils.isNotBlank(oldCountOldCountMap)){
          totalOldCount = oldCountOldCountMap.toLong + freshness.oldCount
        }else{
          // - 若不存在，则直接写入
          totalOldCount = freshness.oldCount
        }

        HbaseUtil.putMapData(tableName,rowkey,cfName,Map(
          channelIdColName -> freshness.channelID,
          dateColName -> freshness.date,
          newCountColName -> totalNewCount,
          oldCountColName -> totalOldCount
        ))
    }
  }
}
