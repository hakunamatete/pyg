package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._

/**
  * 实时频道热点分析业务开发
  * 频道热点，就是要统计频道被访问（点击）的数量。
  * 分析得到以下的数据：
  * 频道ID 访问数量
  * 频道ID1 128
  * 频道ID2 401
  * 频道ID3 501
  */
object ChannelRealHotTask {

  // 1. 添加一个`ChannelRealHot`样例类，它封装要统计的两个业务字段：频道ID（channelID）、访问数量（visited）
  case class ChannelRealHot(var channelId:String, var visited:Long)

  // 2. 在`ChannelRealHotTask`中编写一个`process`方法，接收预处理后的`DataStream`
  def process(clicklogWideDataStream:DataStream[ClickLogWide]) ={

    // 3. 使用map算子，将`ClickLog`对象转换为`ChannelRealHot`
    val channelRealHotDataStream: DataStream[ChannelRealHot] = clicklogWideDataStream.map {
      clicklog =>
        ChannelRealHot(clicklog.channelID, clicklog.count)
    }

    // 4. 按照频道ID进行分流
    val groupedDataStream: KeyedStream[ChannelRealHot, String] = channelRealHotDataStream.keyBy(_.channelId)

    // 5. 划分时间窗口（3秒一个窗口）
    val windowedDataStream: WindowedStream[ChannelRealHot, String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    // 6. 执行reduce合并计算
    val reducedDataStream: DataStream[ChannelRealHot] = windowedDataStream.reduce {
      (realHost1, realHost2) =>
        ChannelRealHot(realHost2.channelId, realHost1.visited + realHost2.visited)
    }
    reducedDataStream.print()

    // 7. 将合并后的数据下沉到hbase
    reducedDataStream.addSink{
      realHot =>
        // hbase的相关字段名创建出来
        val tableName = "channel_realhot"
        val cfName = "info"
        // 频道ID（channelID）、访问数量（visited）
        val rowkey = realHot.channelId
        val channelIdColName = "channelId"
        val visitedColName = "visited"

        // 判断hbase中是否已经存在结果记录
        val visitedInHBase: String = HbaseUtil.getData(tableName,rowkey,cfName,visitedColName)

        var totalVisited = 0L
        if(StringUtils.isNotBlank(visitedInHBase)){
          // 若存在，则获取后进行累加
          totalVisited = visitedInHBase.toLong + realHot.visited
        }else{
          // 若不存在，则直接写入
          totalVisited = realHot.visited
        }

        // 将数据写入到hbase
        HbaseUtil.putMapData(tableName,rowkey,cfName,Map(
          channelIdColName -> realHot.channelId,
          visitedColName -> totalVisited
        ))
    }
  }
}
