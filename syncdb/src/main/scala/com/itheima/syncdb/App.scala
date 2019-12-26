package com.itheima.syncdb

import java.util.Properties

import com.itheima.syncdb.bean.{Canal, HBaseOperation}
import com.itheima.syncdb.task.PreprocessTask
import com.itheima.syncdb.util.{GlobalConfigUtil, HbaseUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
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

    val canalDataStream: DataStream[Canal] = KafkaDataStream.map(
      json =>
        Canal(json)
    )
    canalDataStream.print()

    // 添加flink的水印处理 , 允许得最大延迟时间是2S
    val watermarkDataStream = canalDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Canal] {
      var currentTimestamp: Long = 0L
      val maxDelayTime = 2000L
      var watermark: Watermark = null
      // 获取当前的水印
      override def getCurrentWatermark = {
        watermark = new Watermark(currentTimestamp - maxDelayTime)
        watermark
      }
      // 时间戳抽取操作
      override def extractTimestamp(t: Canal, l: Long) = {
      val timeStamp = t.timestamp
      currentTimestamp = Math.max(timeStamp, currentTimestamp)
      currentTimestamp
    }
  })

    val hbaseOperationDataStream: DataStream[HBaseOperation] = PreprocessTask.process(watermarkDataStream)

    hbaseOperationDataStream.print()

    hbaseOperationDataStream.addSink{
      op =>
        op.opType match {
          case "DELETE" => HbaseUtil.deleteData(op.tableName,op.rowkey,op.cfName)
          case _ => HbaseUtil.putData(op.tableName,op.rowkey,op.cfName,op.colName,op.colValue)
        }
    }

    env.execute("SyncDBApp")
  }
}
