package sparkSQL

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object Demo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Demo5")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 通过JDBC的方式读取MySQL中的数据
    sqlContext.read.format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://LumiaO:3306/ChinaRegion",
          "driver" -> "com.mysql.jdbc.Driver",
          "user" -> "lumia",
          "dbtable" -> "province",
          "password" -> "1044740758"
        )
      ).load().registerTempTable("regionTable")

    val provinceDF = sqlContext.sql(
      """
        |select * from regionTable where name like "%省"
        |
        |""".stripMargin)

    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "lumia")
    prop.put("password", "1044740758")

    provinceDF.write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.31.81:3306/test"
        , "regionTable"
        , prop)

  }
}
