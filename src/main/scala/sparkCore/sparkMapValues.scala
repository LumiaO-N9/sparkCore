package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    val list = List("a", "b", "c", "d")

    val rdd = sc.parallelize(list)
    rdd.map((_, 1)).mapValues(_ + 1).foreach(println)

  }
}
