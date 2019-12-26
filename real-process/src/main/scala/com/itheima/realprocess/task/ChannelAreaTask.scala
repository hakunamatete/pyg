package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HbaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ChannelAreaTask {


  //2. 添加一个`ChannelArea`样例类，它封装要统计的四个业务字段：频道ID（channelID）、地域（area）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
  case class ChannelArea(var channelID:String,
                         var area:String,
                         var date:String,
                         var pv:Long,
                         var uv:Long,
                         var newCount:Long,
                         var oldCount:Long)
  //3. 在`ChannelAreaTask`中编写一个`process`方法，接收预处理后的`DataStream`
  def process(clicklogWideDataStream:DataStream[ClickLogWide]) = {

    //4. 使用`flatMap`算子，将`ClickLog`对象转换为三个不同时间维度`ChannelArea`
    val channelAreaDataStream: DataStream[ChannelArea] = clicklogWideDataStream.flatMap {
      clicklog =>
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
        List(
          ChannelArea(clicklog.channelID,
            clicklog.address,
            clicklog.yearMonthDayHour,
            clicklog.count,
            clicklog.isHourNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isHourNew)), // 小时维度
          ChannelArea(clicklog.channelID,
            clicklog.address,
            clicklog.yearMonthDay,
            clicklog.count,
            clicklog.isDayNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isDayNew)), // 天维度
          ChannelArea(clicklog.channelID,
            clicklog.address,
            clicklog.yearMonth,
            clicklog.count,
            clicklog.isMonthNew,
            clicklog.isNew,
            isOld(clicklog.isNew, clicklog.isMonthNew)) // 月维度
        )
    }
    //5. 按照`频道ID`、`时间`、`地域`进行分流
    val groupedByDataStream = channelAreaDataStream.keyBy {
      area =>
        area.channelID + area.date + area.area
    }
    //6. 划分时间窗口（3秒一个窗口）
    val windowDataStream = groupedByDataStream.timeWindow(Time.seconds(3))
    //7. 执行reduce合并计算
    val reduceDataStream = windowDataStream.reduce {
      (area1, area2) =>
        ChannelArea(area2.channelID, area2.area, area2.date, area1.pv + area2.pv, area1.uv + area2.uv,
          area1.newCount + area2.newCount, area1.oldCount + area2.oldCount)
    }

    //8. 打印测试
    reduceDataStream.print()

    //9. 将合并后的数据下沉到hbase
    reduceDataStream.addSink{
      area =>
        //- 准备hbase的表名、列蔟名、rowkey名、列名
        val tableName = "channel_area"
        val rowkey = s"${area.channelID}:${area.date}:${area.area}"
        val cfName = "info"

        // 频道ID（channelID）、地域（area）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
        val channelIdColName = "channelID"
        val areaColName = "area"
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
          totalPv = pvInHBase.toLong + area.pv
        } else {
          totalPv = area.pv
        }
        if(StringUtils.isNotBlank(uvInHBase)) {
          totalUv = uvInHBase.toLong + area.uv
        } else {
          totalUv = area.uv
        }
        if(StringUtils.isNotBlank(newInHBase)) {
          totalNewCount = newInHBase.toLong + area.newCount
        } else {
          totalNewCount = area.newCount
        }
        if(StringUtils.isNotBlank(oldInHBase)) {
          totalOldCount = oldInHBase.toLong + area.oldCount
        } else {
          totalOldCount = area.oldCount
        }
        HbaseUtil.putMapData(tableName,rowkey,cfName,Map(
          // 频道ID（channelID）、地域（area）、日期（date）pv、uv、新用户（newCount）、老用户（oldCount）
          channelIdColName -> area.channelID,
          areaColName -> area.area,
          dateColName -> area.date,
          pvColName -> totalPv.toString,
          uvColName -> totalUv.toString,
          newCountColName -> totalNewCount.toString,
          oldCountColName -> totalOldCount.toString
        ))
    }
  }
}
