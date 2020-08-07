import org.apache.spark.{SparkConf, SparkContext}

object sparkSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    val studnetRdd = sc.textFile("data/students.txt")
    //    val scoreRdd = sc.textFile("data/score.txt")
    /**
     * sample 取样函数
     * withReplacement: Boolean, 是否放回
     * fraction: Double, 取得比例
     * seed: Long 步长
     */
    val sampleRdd = studnetRdd.sample(true, 0.1)
    println(sampleRdd.count())
  }

}
