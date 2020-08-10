package sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo1")
    val sc = new SparkContext(conf)
    // 创建sqlContext
    val sqlContext = new SQLContext(sc)
    sqlContext
      // 读数据
      .read
      // json 表示读取json格式的数据
      //  .json("data/json/students.json")
      // 或者 直接使用format 加 load 读取
      .format("json")
      .load("data/json/students.json")
      // 打印数据
      //      .foreach(println)  不打印列名
      // show 方法表示打印数据 类似于 RDD的 foreach(println)
      // 区别： show 方法会打印出列名
      // action 算子
      // 参数： numRows, truncate
      // numRows ： 表示限制输出数据的行数
      // truncate： 默认为true 表示截断输出，最多每列只显示20个字符
      .show(false)

  }
}
