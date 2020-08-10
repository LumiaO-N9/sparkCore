package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //    println("helloWorld!")
    // 1、读取数据
    // 配置Spark
    // 创建SparkContext

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd =  sc.textFile("data/words")

    //    rdd.foreach(println)
    // 2、使用算子进行操作
    //java,scala,python => (java,1) (scala,1) (python,1)
    rdd.flatMap(line=>line.split(",")).map(line=>(line,1)).reduceByKey(_+_).foreach(println)

  }

}
