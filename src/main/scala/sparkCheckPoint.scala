import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.StorageLevels

object sparkCheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/checkpoint")
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

    var joinRdd = student.join(score)
      .map(line => {
        val id = line._1
        val tup = line._2
        val studentInfo = tup._1
        val scoreInfo = tup._2
        val name = studentInfo.split(",")(1)
        val score = scoreInfo.split(",")(2).toInt
        (id + "," + name, score)
      })
    // 设置缓存
    //    joinRdd = joinRdd.cache()
    joinRdd = joinRdd.persist(StorageLevels.MEMORY_ONLY)
    joinRdd.checkpoint()
    // 求总分
    joinRdd.reduceByKey(_ + _)
      .foreach(println)


    // 求一个 学生的平均分
    // (1500100898,祁高旻,CompactBuffer(29, 1, 13, 75, 21, 99))
    joinRdd.groupByKey().map(kv => {
      val key = kv._1
      val values = kv._2
      val sumScore = values.sum
      val count = values.count(x => true)
      val avg = sumScore / count
      (key, avg)
    }).foreach(println)
  }
}
