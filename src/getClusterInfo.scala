import org.apache.spark.{SparkContext, SparkConf}

object getClusterInfo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("getClusterInfo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val carsInfo = input.map(line => line.split("\t")(0).split(",")).groupBy(_(0))
    val carsCount = carsInfo.count()
    val perCarClusterCount = carsInfo.map(car => car._2.size).sum()
    println(perCarClusterCount / carsCount, carsCount)
  }
}
