package sparkStreaming

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object sparkStreamingDemo1 {
  def main(args: Array[String]): Unit = {
    // 创建spark 配置
    val conf = new SparkConf()
      // []中括号内的 数值 表示会启动多少个executor
      // spark Streaming至少需要两个executor，一个进行数据接收，一个进行数据处理
      .setMaster("local[2]").setAppName("ssDemo1")
    // 创建spark 上下文
    val sc = new SparkContext(conf)

    // 创建Spark Streaming 上下文
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // 首先需要在任意一台linux 虚拟机上安装 nc 工具
    // yum install nc
    // nc -l 9999 # 表示在 9999 端口 建立socket
    // 建立Socket 连接
    // 参数： hostname : 指定一个主机名 或者直接指定一个ip地址
    // port：指定 nc 命令使用的端口号
    // storageLevel：表示缓存级别
    val socketSS = ssc.socketTextStream("master", 9999, StorageLevels.MEMORY_AND_DISK)

    // java,scala => java
    //               scala

    socketSS.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    // 启动 Spark Streaming 程序
    ssc.start()
    // 等待终止
    ssc.awaitTermination()
    // 停止 Spark Streaming 程序
    ssc.stop()
  }


}
