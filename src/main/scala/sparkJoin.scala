import org.apache.spark.{SparkConf, SparkContext}

object sparkJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    /*
    * local 模式下，textfile 读取的是本地文件
    * on Yarn 模式下，textFile 读取的是HDFS上的文件
     */
    val studentRdd = sc.textFile("data/students.txt")
    val scoreRdd = sc.textFile("data/score.txt")

    val student = studentRdd.map(line => {
      val splitArr = line.split(",")
      val id = splitArr(0)
      (id, line)
    })

    val score = scoreRdd.map(line => {
      val splitArr = line.split(",")
      val id = splitArr(0)
      (id, line)
    })

    val joinRdd = student.join(score)
    joinRdd.map(line => {
      val id = line._1
      val tup = line._2
      val studentInfo = tup._1
      val scoreInfo = tup._2
      val name = studentInfo.split(",")(1)
      val score = scoreInfo.split(",")(2).toInt
      (id + "," + name, score)
    }).reduceByKey(_ + _).foreach(println)
    //    joinRdd.foreach(println)

  }
}
