package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * 分析出来中国移动、中国联通、中国电信等运营商的指标。来分析，流量的主要来源是哪个运营商的，这样就可以进行较准确的网络推广。
  * 频道ID 运营商 时间 PV UV 新用户 老用户
  * 频道1          201809 1000 300 0 300
  * 频道1 中国联通 20180910 123 1 0 1
  * 频道1 中国电信 2018091010 55 2 2 0
  */
object ChannelNetworkTask {

  // 2. 添加一个`ChannelNetwork`样例类，它封装要统计的四个业务字段：频道ID（channelID）、运营商（network）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
  case class ChannelNetwork(var channelID: String,
                            var network: String,
                            var date: String,
                            var pv: Long,
                            var uv: Long,
                            var newCount: Long,
                            var oldCount: Long)

  // 3. 在`ChannelNetworkTask`中编写一个`process`方法，接收预处理后的`DataStream`
  def process(clicklogWideDataStream: DataStream[ClickLogWide]) = {

    // 4. 使用`flatMap`算子，将`ClickLog`对象转换为三个不同时间维度`ChannelNetwork`
    val channelNetworkDataStream = clicklogWideDataStream.flatMap {
      clicklog =>
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelNetwork(clicklog.channelID,
            clicklog.network,
            clicklog.yearMonthDayHour,
            clicklog.count,clicklog.isHourNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isHourNew)), // 小时维度
          ChannelNetwork(clicklog.channelID,
            clicklog.network,
            clicklog.yearMonthDay,
            clicklog.count,
            clicklog.isDayNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isDayNew)), // 天维度
          ChannelNetwork(clicklog.channelID,
            clicklog.network,
            clicklog.yearMonth,
            clicklog.count,
            clicklog.isMonthNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isMonthNew)) // 月维度
        )
    }
    // 5. 按照`频道ID`、`时间`、`运营商`进行分流
    val groupedDataStream = channelNetworkDataStream.keyBy {
      network =>
        network.channelID + network.date + network.network
    }

    // 6. 划分时间窗口（3秒一个窗口）
    val windowDataStream = groupedDataStream.timeWindow(Time.seconds(3))

    // 7. 执行reduce合并计算
    val reduceDataStream: DataStream[ChannelNetwork] = windowDataStream.reduce {
      (network1, network2) =>
        ChannelNetwork(network2.channelID,
          network2.network,
          network2.date,
          network1.pv + network2.pv,
          network1.uv + network2.uv,
          network1.newCount + network2.newCount,
          network1.oldCount + network2.oldCount)
    }
    // 8. 打印测试
    reduceDataStream.print()

    //9. 将合并后的数据下沉到hbase
    reduceDataStream.addSink{
      network =>
        //- 准备hbase的表名、列蔟名、rowkey名、列名
        val tableName = "channel_network"
        val rowkey = s"${network.channelID}:${network.date}:${network.network}"
        val cfName = "info"

        // 频道ID（channelID）、运营商（network）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
        val channelIdColName = "channelID"
        val networkColName = "network"
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
          totalPv = pvInHBase.toLong + network.pv
        } else {
          totalPv = network.pv
        }
        if(StringUtils.isNotBlank(uvInHBase)) {
          totalUv = uvInHBase.toLong + network.uv
        } else {
          totalUv = network.uv
        }
        if(StringUtils.isNotBlank(newInHBase)) {
          totalNewCount = newInHBase.toLong + network.newCount
        } else {
          totalNewCount = network.newCount
        }
        if(StringUtils.isNotBlank(oldInHBase)) {
          totalOldCount = oldInHBase.toLong + network.oldCount
        } else {
          totalOldCount = network.oldCount
        }
        HbaseUtil.putMapData(tableName,rowkey,cfName,Map(
          // 频道ID（channelID）、地域（area）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
          channelIdColName -> network.channelID,
          networkColName -> network.network,
          dateColName -> network.date,
          pvColName -> totalPv.toString,
          uvColName -> totalUv.toString,
          newCountColName -> totalNewCount.toString,
          oldCountColName -> totalOldCount.toString
        ))
    }
  }
}
