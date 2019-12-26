package com.itheima.batch_process

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import com.itheima.batch_process.task.{PaymethodTask, PreprocessTask}
import com.itheima.batch_process.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object App {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val hbaseJsonDataSet: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("mysql.pyg.commodity"))

    val orderRecordDataSet: DataSet[OrderRecord] = hbaseJsonDataSet.map {
      tuple2 =>
        OrderRecord(tuple2.f1)
    }

    // 对数据进行拓宽
    val orderRecordWideDataSet: DataSet[OrderRecordWide] = PreprocessTask.process(orderRecordDataSet)

    // 按照不同的时间维度统计不同的支付方式的金额和数量
    PaymethodTask.process(orderRecordWideDataSet)

//    env.execute("BatchProcessApp")
  }
}
