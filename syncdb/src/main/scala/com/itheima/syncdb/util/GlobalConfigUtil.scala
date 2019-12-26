package com.itheima.syncdb.util

import com.typesafe.config.{Config, ConfigFactory}

object GlobalConfigUtil {

  private val config: Config = ConfigFactory.load();

  /*------------------
   * Kafka配置
   *------------------*/
  def bootstrapServers = config.getString("bootstrap.servers")
  def zookeeperConnect = config.getString("zookeeper.connect")
  def inputTopic = config.getString("input.topic")
  def groupId = config.getString("group.id")
  def enableAutoCommit = config.getString("enable.auto.commit")
  def autoCommitIntervalMs = config.getString("auto.commit.interval.ms")
  def autoOffsetReset = config.getString("auto.offset.reset")


  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
  }
}
