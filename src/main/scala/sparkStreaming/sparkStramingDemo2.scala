package sparkStreaming

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparkSQL.Demo4.Student

object sparkStramingDemo2 {
  def main(args: Array[String]): Unit = {
    // 创建spark 配置
    val conf = new SparkConf()
      // []中括号内的 数值 表示会启动多少个executor
      // spark Streaming至少需要两个executor，一个进行数据接收，一个进行数据处理
      .setMaster("local[2]").setAppName("ssDemo1")
    // 创建spark 上下文
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Durations.seconds(5))

    val ds = ssc.socketTextStream("master", 9999, StorageLevels.MEMORY_ONLY)

    // 使用foreachRDD 就可以将 DStream 转换成 RDD
    // RDD又能转换成DF
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    ds.foreachRDD(rdd => {
      val df = rdd.map(_.split(","))
        .map(arr => Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4)))
        .toDF()

      df.registerTempTable("students")
      sqlContext.sql(
        """
          |select * from students
          |""".stripMargin).show()

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
