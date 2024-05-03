import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.rdd.RDD

object task3 {
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: spark-submit --class task3 hw1.jar <review_file> <business_file> <output_filepath_question_a> <output_filepath_question_b>")
      System.exit(1)
    }

    val reviewFile = args(0)
    val businessFile = args(1)
    val outputFilePathA = args(2)
    val outputFilePathB = args(3)

    val spark = SparkSession.builder().appName("Assignment1_Task3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val reviewData = spark.read.json(reviewFile).as[(String, Double)].rdd.map {
      case (business_id, stars) => (business_id, stars)
    }

    val businessData = spark.read.json(businessFile).as[(String, String)].rdd.map {
      case (business_id, city) => (business_id, city)
    }

    val joinedData = reviewData.join(businessData)

    val sumCounts = joinedData.map {
      case (_, (stars, city)) => (city, (stars, 1.0))
    }.reduceByKey {
      case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }

    val averageStars = sumCounts.mapValues {
      case (sum, count) => sum / count
    }

    val sortedResults = averageStars.sortBy {
      case (city, stars) => (-stars, city)
    }.map {
      case (city, stars) => s"$city,$stars"
    }.collect()

    import java.io.PrintWriter
    new PrintWriter(outputFilePathA) {
      write("city,stars\n")
      sortedResults.foreach(write)
      close()
    }

    val startM1 = System.currentTimeMillis()
    val top10CitiesM1 = averageStars.collect().sortWith {
      case ((city1, stars1), (city2, stars2)) => stars1 > stars2 || (stars1 == stars2 && city1 < city2)
    }.take(10)
    val durationM1 = System.currentTimeMillis() - startM1

    val startM2 = System.currentTimeMillis()
    val top10CitiesM2 = averageStars.sortBy {
      case (city, stars) => (-stars, city)
    }.take(10)
    val durationM2 = System.currentTimeMillis() - startM2

    val results = Map(
      "m1" -> durationM1,
      "m2" -> durationM2,
      "reason" -> "Sorting in native Scala (M1) might be slower than Spark's distributed sorting (M2) especially for large datasets. ..."
    )

    new PrintWriter(outputFilePathB) {
      write(results.mkString("\n"))
      close()
    }

    spark.stop()
  }
}
