package sparkStreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object test1 {
  def main(args: Array[String]): Unit = {
    //local[2]  spark streaming 程序需要多个Executor
    val conf = new SparkConf().setAppName("stream").setMaster("local[2]")
    val sc = new SparkContext(conf)


    // Durations.seconds(5)  封装rdd间隔时间    5秒处理一次
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    //通过访问服务创建一个DStream
    //在nnode1执行 nc -lk 8888
    val ds = ssc.socketTextStream("master", 9999)

    //上层是DS之前的转换 底层是RDD之间的转换    每隔5秒执行一次
    val mapDS = ds.map((_, 1))

    val countDS = mapDS.reduceByKey(_ + _)

    //相当于action算子   每隔5秒启动一个job任务
    countDS.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
