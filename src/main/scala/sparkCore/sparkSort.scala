package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)
    /**
     * sortBy 分区类算子，使用前需要构建 K-V 的数据
     * 参数 ascending 默认等于true 默认是升序排序
     * 当它等于false 表示降序
     */
    val studentRDD = sc.textFile("data/score.txt")
    studentRDD.map(line => {
      val splitArr = line.split(",")
      val id = splitArr(0)
      val score = splitArr(2).toInt
      (id, score)
    })
      //  .sortByKey()
      .sortBy(_._2, ascending = false)
      .foreach(println)
  }
}
