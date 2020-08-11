package sparkSQL

import java.nio.file.FileSystem

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import tachyon.client.file.FileSystemContext

object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo4")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val studentDF = sc.textFile("data/students.txt")
      .map(_.split(","))
      .map(arr => Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4)))
      .toDF()

    val scoreDF = sc.textFile("data/score.txt")
      .map(_.split(","))
      .map(arr => Score(arr(0), arr(1), arr(2).toInt))
      .toDF()

    // 使用SQL 语法 进行数据查询
    // 1、首先需要构建好DataFrame
    // 2、使用registerTempTable函数 将 DF 注册成表
    // 3、使用sqlContext.sql(" sql 查询语句 ")

    studentDF.registerTempTable("students")
    scoreDF.registerTempTable("scores")


//    sqlContext.sql("select * from students").show(10)
//
//    sqlContext.sql("select * from students a left join scores b on a.id = b.id")
//      .show(20)

//    hiveContext.refreshTable("students")
    // 求每个学生的总分
    hiveContext.sql(
      """
        |select a.id,a.name,a.clazz,sum(score) as total_score from
        |students a
        |left join scores b
        |on a.id = b.id
        |group by a.id,a.name,a.clazz
        |""".stripMargin).registerTempTable("totalScore")

//    // 求每个学生的平均成绩
//    sqlContext.sql(
//      """
//        |select a.id
//        |,a.name
//        |,a.clazz
//        |,round(sum(score)/count(score),2) as avg_score
//        |from
//        |students a
//        |left join scores b
//        |on a.id = b.id
//        |group by a.id,a.name,a.clazz
//        |""".stripMargin)
//      // Spark SQL 任务默认产生200个task，会生成200个文件
//      // 为了避免产生过多的小文件，使用repartition进行重分区
//      .repartition(2)
//      // 使用parquet格式存储的优点
//      // 1、节省空间，会以一种压缩的方式储存数据
//      // 2、可以直接保存数据的元数据，下次读取的时候直接可以转换成DF
//      // 缺点
//      // 1、不方便直接查看
//      .write
//      .format("parquet")
//      // save的四种模式
//      // error 默认的 目录存在就报错
//      // append 追加模式
//      // overwrite 覆盖模式
//      // ignore 目录存在直接跳过
//      .mode("append")
//      .save("data/parquet")

//    val sDF = sqlContext.read.format("parquet").load("data/parquet")
//    sDF.filter(sDF("clazz") === "文科一班")
//      .sort(sDF("avg_score").desc)
//      .show()


    // 查看每个班级总分排名前十的学生
    // row_number 开窗函数
    hiveContext.sql(
      """
        |select id,name,clazz,total_score,
        | row_number() over (partition by clazz order by total_score) as rank
        | from totalScore
        |""".stripMargin).show(100)

  }

  case class Student(
                      id: String,
                      name: String,
                      age: Int,
                      gender: String,
                      clazz: String
                    )

  case class Score(
                    id: String,
                    course_id: String,
                    score: Int
                  )

}
