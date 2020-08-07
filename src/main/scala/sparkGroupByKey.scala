import org.apache.spark.{SparkConf, SparkContext}

object sparkGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    /*
    * local 模式下，textfile 读取的是本地文件
    * on Yarn 模式下，textFile 读取的是HDFS上的文件
     */
    val studentRdd = sc.textFile("data/students.txt")


    /**
     * groupByKey 算子 它是一个分区类的转换算子
     * 如果要使用它，首先需要手动构建好（key,value）格式的数据
     */
    studentRdd.map(line => {
      val arr = line.split(",")
      val clazz = arr(4)
      (clazz, 1)
    })
//      .groupByKey()
//      .map(tup => {
//        val key = tup._1
//        val countP = tup._2.sum
//        (key,countP)
//      })
      .reduceByKey(_+_)
      .foreach(println)
  }
}
