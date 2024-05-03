import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object task1 {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-submit --class task1 hw1.jar <review_filepath> <output_filepath>")
      System.exit(1)
    }

    val reviewFilepath = args(0)
    val outputFilepath = args(1)

    val spark = SparkSession.builder()
      .appName("Assignment1_Task1")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val reviews = spark.read.json(reviewFilepath).rdd

    // A. The total number of reviews
    val n_review = reviews.count()

    // B. The number of reviews in 2018
    val n_review_2018 = reviews.filter(row => row.getAs[String]("date").startsWith("2018")).count()

    // C. The number of distinct users who wrote reviews
    val n_user = reviews.map(row => row.getAs[String]("user_id")).distinct().count()

    // D. The top 10 users who wrote the largest numbers of reviews
    val top10_user = reviews.map(row => (row.getAs[String]("user_id"), 1))
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1))
      .take(10)

    // E. The number of distinct businesses that have been reviewed
    val n_business = reviews.map(row => row.getAs[String]("business_id")).distinct().count()

    // F. The top 10 businesses that had the largest numbers of reviews
    val top10_business = reviews.map(row => (row.getAs[String]("business_id"), 1))
      .reduceByKey(_ + _)
      .sortBy(x => (-x._2, x._1))
      .take(10)

    val result = Map(
      "n_review" -> n_review,
      "n_review_2018" -> n_review_2018,
      "n_user" -> n_user,
      "top10_user" -> top10_user,
      "n_business" -> n_business,
      "top10_business" -> top10_business
    )

    val resultDF = Seq(result).toDF()
    resultDF.write.json(outputFilepath)

    sc.stop()
  }
}
