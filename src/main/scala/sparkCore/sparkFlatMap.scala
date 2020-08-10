package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object sparkFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)
    val list = List("python,java,scala", "hadoop,hbase,spark,hive")
    val rdd1 = sc.parallelize(list)

    /**
     * flatMap 算子 传入一行，可能返回多行
     * 实际上是分两步
     * 第一步：做一个map操作
     * 第二步：将map操作返回的结果做一个展开
     */
    rdd1.flatMap(_.split(","))
      .foreach(println)
  }
}
