package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 6)
    println(sc.parallelize(list).count())

    val rdd = sc.parallelize(list)

    /**
     * 使用的时候需要注意，当数据量过大的时候，容易造成OOM
     */
    //    rdd.collect()
  }
}
