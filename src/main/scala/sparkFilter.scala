import org.apache.spark.{SparkConf, SparkContext}

object sparkFilter {
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
     * filter 算子，需要返回一个bool类型 true/false
     * true 表示保留，false 表示过滤
     */
    studentRdd.map(_.split(",").toList).filter(arr=>{
      "女".equals(arr(3))
    }).foreach(println)

  }
}
