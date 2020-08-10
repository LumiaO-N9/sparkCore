package sparkSQL

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    // 如何从txt文件构建DF
    val conf = new SparkConf().setMaster("local").setAppName("Demo2")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentRDD = sc.textFile("data/students.txt")
    // RDD to DF 需要先导入隐式转换包
    // 第一种方式 使用样例类 然后使用toDF函数可直接将RDD转换成DF
    import sqlContext.implicits._ // 局部导入
    val studentDF = studentRDD.map(_.split(","))
      .map(arr => Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4)))
      .toDF()
    studentDF.show(100)
    // count() min() avg()
    //    studentDF.groupBy("clazz").count().show()
    //    studentDF.groupBy("clazz").max("age").show()
    //    studentDF.groupBy("clazz").min("age").show()

    // RDD to DF 第二种方式
    // 首先需要构建一个schema 对列进行描述
    // 然后需要将RDD的每一行数据变成Row对象
    // 最后调用createDataFrame函数，创建DF
    val schemaList = List("id", "name", "age", "gender", "clazz")
    val schema =
      StructType(
        schemaList.map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = studentRDD.map(_.split(","))
      .map(arr => Row(arr(0), arr(1), arr(2), arr(3), arr(4)))

    val rDF = sqlContext.createDataFrame(rowRDD, schema)
    rDF.show(10)

    // 第三种： 直接构建好每一行为元组的RDD，然后可以在使用toDF的时候指定列名
    studentRDD.map(_.split(","))
      .map(arr => (arr(0), arr(1), arr(2), arr(3), arr(4)))
      .toDF("id", "name", "age", "gender", "clazz")
      .filter("age>23").show()
  }

  case class Student(
                      id: String,
                      name: String,
                      age: Int,
                      gender: String,
                      clazz: String
                    )

}
