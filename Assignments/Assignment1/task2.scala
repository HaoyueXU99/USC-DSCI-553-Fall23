import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

object task2 {

  class CustomPartitioner(partitions: Int) extends Partitioner {
    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      k.hashCode % numPartitions
    }
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: spark-submit --class task2 hw1.jar <review_filepath> <output_filepath> <n_partition>")
      System.exit(1)
    }

    val reviewFilepath = args(0)
    val outputFilepath = args(1)
    val nPartition = args(2).toInt

    val spark = SparkSession.builder.appName("Assignment1_Task2").getOrCreate()

    import spark.implicits._

    val reviews = spark.read.json(reviewFilepath)
      .map(row => row.getAs[Map[String, String]]("business_id"))
      .rdd

    // Default Partitioning
    val defaultStart = System.currentTimeMillis

    val top10_business_default_RDD = reviews.map(x => (x("business_id"), 1))
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1))

    top10_business_default_RDD.take(10)

    val defaultEnd = System.currentTimeMillis

    val defaultNPartitions = top10_business_default_RDD.getNumPartitions
    val defaultNItems = top10_business_default_RDD.glom().map(_.length).collect()

    val defaultDuration = defaultEnd - defaultStart

    // Custom Partitioning
    val customStart = System.currentTimeMillis

    val reviewsCustom = reviews.map(x => (x("business_id"), 1))
      .partitionBy(new CustomPartitioner(nPartition))

    val top10_business_custom_RDD = reviewsCustom
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1))

    top10_business_custom_RDD.take(10)

    val customEnd = System.currentTimeMillis

    val customNPartitions = top10_business_custom_RDD.getNumPartitions
    val customNItems = top10_business_custom_RDD.glom().map(_.length).collect()

    val customDuration = customEnd - customStart

    val results = Map(
      "default" -> Map(
        "n_partition" -> defaultNPartitions,
        "n_items" -> defaultNItems.toList,
        "exe_time" -> defaultDuration.toDouble
      ),
      "customized" -> Map(
        "n_partition" -> customNPartitions,
        "n_items" -> customNItems.toList,
        "exe_time" -> customDuration.toDouble
      )
    )

    val resultsDF = Seq(results).toDF()
    resultsDF.write.json(outputFilepath)

    spark.stop()
  }
}
