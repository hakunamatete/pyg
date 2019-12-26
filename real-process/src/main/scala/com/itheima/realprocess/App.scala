package com.itheima.realprocess

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.itheima.realprocess.bean.{ClickLog, ClickLogWide, Message}
import com.itheima.realprocess.task.ChannelFreshnessTask.ChannelFreshness
import com.itheima.realprocess.task.ChannelRealHotTask.ChannelRealHot
import com.itheima.realprocess.task._
import com.itheima.realprocess.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

object App {

  def main(args: Array[String]): Unit = {
    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // Flink默认的开发环境并行度
    env.setParallelism(3)

    ////////
    //
    // 保证程序长时间运行的安全性进行checkpoint操作
    //
    // 5秒启动一次checkpoint
    env.enableCheckpointing(5000)
    // 设置checkpoint只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // checkpoint超时的时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 允许的最大checkpoint并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://lxl1:8020/user/flink-checkpoint/"))
    //
    // 整合Kafka
    //
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    properties.setProperty("zookeeper.connect", GlobalConfigUtil.zookeeperConnect)
    properties.setProperty("group.id", GlobalConfigUtil.groupId)
    properties.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    properties.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    // 配置下次重新消费的话，从哪里开始消费
    // latest：从上一次提交的offset位置开始的
    // earlist：从头开始进行（重复消费数据）
    properties.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)
    // 配置序列化和反序列化
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)


    //properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
    GlobalConfigUtil.inputTopic,
      new SimpleStringSchema(),
      properties
    )


    val KafkaDataStream: DataStream[String] = env.addSource(consumer)


    // 将JSON的数据解析成一个元组
    val messageDataStream = KafkaDataStream.map {
      message =>
        // 使用map算子，将kafka中消费到的数据，使用FastJSON准换为JSON对象
        val jsonObject = JSON.parseObject(message)
        val timestamp = jsonObject.getLong("timestamp")
        val count = jsonObject.getLong("count")
        val msg = jsonObject.getString("message")
        // 将json数据封装到Message样例类里面
        Message(count,timestamp,ClickLog(msg))
    }

    // 添加flink的水印处理 , 允许得最大延迟时间是2S
    val watermarkDataStream: DataStream[Message] = messageDataStream.assignTimestampsAndWatermarks(new
        AssignerWithPeriodicWatermarks[Message] {
      var currentTimestamp: Long = 0L
      val maxDelayTime = 2000L
      var watermark: Watermark = null
      // 获取当前的水印
      override def getCurrentWatermark = {
        watermark = new Watermark(currentTimestamp - maxDelayTime)
        watermark
      }
      // 时间戳抽取操作
      override def extractTimestamp(t: Message, l: Long) = {
      val timeStamp = t.timestamp
      currentTimestamp = Math.max(timeStamp, currentTimestamp)
      currentTimestamp
    }
  })
    // 在App中调用预处理任务的 process 方法。
    val clicklogWideDataStream: DataStream[ClickLogWide] = PreprocessTask.process(watermarkDataStream)

    // 频道实时热点数据分析
    //ChannelRealHotTask.process(clicklogWideDataStream)

    // 频道统计不同时间维度的PVUV分析
    //ChannelPvUvTask.processHourDim(clicklogWideDataStream)

    // 频道用户新鲜度即分析网站每小时、每天、每月活跃的新老用户占比
    // ChannelFreshnessTask.process(clicklogWideDataStream)

    // 频道统计地域对应的PV、UV、新鲜度
    // ChannelAreaTask.process(clicklogWideDataStream)

    // 频道统计运营商对应的PV、UV、新鲜度
    // ChannelNetworkTask.process(clicklogWideDataStream)

    // 频道统计浏览器对应的PV、UV、新鲜度
     ChannelBrowserTask.process(clicklogWideDataStream)

    env.execute("App")
  }
}
