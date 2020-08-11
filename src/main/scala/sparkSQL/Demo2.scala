package sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo2")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentDF = sqlContext.read.json("data/json/students.json")
    // 输出DF的结构 打印列名 即 元数据
    studentDF.printSchema()
    studentDF.show()

    // select 转换算子 提取指定的列
    // 第一种方式，直接传入列名
    studentDF.select("age").show()
    // 第二种方式，直接传入列
    studentDF.select(studentDF("name"), studentDF("age") + 1).show()

    // filter 转换算子
    // 第一种方式 括号里传入的 参数 相当于 where 条件后接的表达式
    studentDF.filter("age>23").show()
    // 第二种方式
    // 用列去比较
    studentDF.filter(studentDF("age") > 23).show()

    // groupBy 转换算子，可以传入多个列
    // 聚合函数 只能先分组再使用
    studentDF
      .groupBy("id", "name", "age")
      .count()
      .show()

    // sort 转换算子
    // 默认升序
    studentDF.sort(studentDF("age")).show()
    // 降序
    studentDF.sort(studentDF("age").desc).show()

    // 去重，去重整行数据都重复的数据
    studentDF.distinct().show()
  }
}
