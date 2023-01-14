// scalastyle:off println
package emrserverless.customjre

import scala.math.random
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession

/** Generates a random dataset and calculates rank over window **/
object SyntheticAnalysis {
  def main(args: Array[String]): Unit = {
    
    // Initiatize Spark session 
    val spark = SparkSession
      .builder
      .appName("EMR Serverless Spark Custom Java Runtime with Custom Images")
      .getOrCreate()
    
    // Randomization input
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(slices, Int.MaxValue).toInt // avoid overflow
    println("Parallelism configured: " +n)

    // Create a list of countries 
    val countries = Seq(
    	"China",
    	"Japan",
    	"Korea",
    	"Russia",
    	"Austria",
    	"India",
    	"Singapore",
    	"Nigeria",
    	"Germany",
    	"Sweden",
    	"London",
    	"Rwanda",
    	"United States",
    	"Mexico",
    	"Peru"
	)

    // Create UDFs to generate random UUIDs 
    val randomUUID = udf(() => java.util.UUID.randomUUID.toString)
    val randomString = udf(() => Random.alphanumeric.filter(_.isLetter).take(100).mkString)
    val randomCountry = udf(() => countries(Random.nextInt(countries.length)))
    val randomTemperature = udf(() => Random.nextInt((134 - (-88)) + 1) + (-88))
    val strangeOffset = udf(() => java.time.ZoneId.of("America/Los_Angeles").getRules.getOffset(java.time.LocalDateTime.parse("1883-11-10T00:00:00")).toString)

    // Register UDFs 
    spark.udf.register("randomUUID", randomUUID.asNondeterministic())
    spark.udf.register("randomString", randomString.asNondeterministic())
    spark.udf.register("strangeOffset", strangeOffset.asNondeterministic())
    spark.udf.register("randomCountry", randomCountry.asNondeterministic())
    spark.udf.register("randomTemperature", randomTemperature.asNondeterministic())

    // Create input dataset with range in argument
    val df = spark.range(0,n) 

    // Create a dataset 
    val random_df = df.select("id").withColumn("country",randomCountry()).withColumn("temperature",randomTemperature()).withColumn("uniform", rand(10L)).withColumn("biguniform", rand(50L)).withColumn("normal", randn(10L)).withColumn("bignormal", randn(50L)).withColumn("uuid", randomUUID()).withColumn("words", randomString()).withColumn("date", date_format(col("current_date"), "yyyy-MM-dd")).withColumn("timestamp", date_format(col("current_timestamp"), "yyyy-MM-dd HH:mm:ss"))

    val calc_df = random_df.withColumn("calculation", when((col("normal") * col("bignormal")) + col("temperature") * col("temperature") > 0,"Positive")
      .when((col("normal") * col("bignormal")) + col("temperature") * col("temperature") === 0,"Zero")
      .otherwise("Negative"))

    // Implement Java functions that will return different results for different JREs

    // More info - https://www.databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html
    val java_df = calc_df.withColumn("timezone",lit((java.time.ZoneId.systemDefault).toString)).withColumn("strangeTimeZoneOffset", strangeOffset())

    // Get Top Countries ranked by Temperature (This is synthetic data)
    val windowSpec  = Window.partitionBy("country").orderBy("temperature")

    val rank_df = java_df.repartition(400).withColumn("rank",rank().over(windowSpec))

    println("Showing countries with highest recorded temperature. This is synthetic data!!")
    rank_df.sort(desc("rank")).show 

    spark.stop()
  }
}
// scalastyle:on println
