package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    /*
    * local 模式下，textfile 读取的是本地文件
    * on Yarn 模式下，textFile 读取的是HDFS上的文件
     */
    val studentRdd = sc.textFile("data/students.txt")
    val scoreRdd = sc.textFile("data/score.txt")

    /**
     * map 输入一条数据，返回一条
     * 输入类型和返回类型 可以自定义
     */

    studentRdd.map(line => line.split(","))
      .map(arr => {
        Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4))
      }).map(student=>{
      val name = student.name
      name
    }).foreach(println)
    //
    //    studentRdd.foreach(println)
    //    scoreRdd.foreach(println)
  }

  case class Student(
                      id: String,
                      name: String,
                      age: Int,
                      gender: String,
                      clazz: String
                    )

}
