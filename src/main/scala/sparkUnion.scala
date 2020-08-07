import org.apache.spark.{SparkConf, SparkContext}

object sparkUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkMap")
    val sc = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4, 5, 6)
    val list2 = List(7, 8, 9)

    val list1Rdd = sc.parallelize(list1)
    val list2Rdd = sc.parallelize(list2)

    list1Rdd.union(list2Rdd).foreach(println)
  }
}
