package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkReduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkReduce").setMaster("local")
    val sc = new SparkContext(conf)

    val list = List(1, 3, 5, 7, 9)
    val rdd = sc.parallelize(list)
    println(rdd.reduce(_ * _))

  }
}
