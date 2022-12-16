// This job finds all the US hospitals, doctors and clinics 

// scalastyle:off println
package emrserverless.prometheus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.sys.process._
import sys.process._

object OpenStreetMapAnalysis extends Serializable {
  def main(args: Array[String]) {
   def start() = {
        val spark = SparkSession.builder().appName("EMR Serverless Spark Prometheus Monitoring with Custom Image").enableHiveSupport().getOrCreate()
        import spark.implicits._
	// Start the prometheus agent created from the custom image 
	val result = Seq("/home/hadoop/runner.sh").!!
        println("runner script")
        val f = Seq("cat","/home/hadoop/runner.sh").!!
        println(f)
        println("is script running?")
        println("ps -ef" #| "grep -i script" !)
        println("prometheus config")
        val z = Seq("cat","/home/hadoop/prometheus.yaml").!!
        println(z)
        println("is prometheus running?")
        println("ps -ef" #| "grep -i prometheus.yaml" !)
        println("is prometheus running 2?")
        val d = Seq("cat","/home/hadoop/prometheus-agent.out").!!
        println(d)
        //val e = Seq("curl","-s","http://localhost:4040/metrics/executors/prometheus").!!
        //println(e)

	// Read data from OpenStreetMap
	val planet_df = spark.read.orc("s3://osm-pds/planet/planet-latest.orc")
	val planet_history_df = spark.read.orc("s3://osm-pds/planet-history/history-latest.orc")
	val changesets_df = spark.read.orc("s3://osm-pds/changesets/changesets-latest.orc")

	// Select relevant columns from nodes
	val node_df = planet_df.select("id","type","tags","lat","lon").where(col("type") === "node")

	// Select relevant columns from ways
	val ways_df = planet_df.select("id", "type", "tags","nds").where(col("type") === "way")

	val ways_filtered_df = ways_df.withColumn("medical_amenity", map_filter(col("tags"),(k, v) => (v.isin("hospital","doctors","clinic") && k.isin("amenity")))).withColumn("is_empty", size(col("medical_amenity")) <= 0)
	
	val nonempty_ways_df = ways_filtered_df.select($"id", $"type", $"tags", $"medical_amenity", $"is_empty",$"nds").withColumn("nd",explode(col("nds"))).where(col("is_empty") === "false").drop("nds")
	nonempty_ways_df.persist

	//Filter nodes with ameneties -> hospital, doctor, clinic 
	val nodes_filtered_df = node_df.withColumn("medical_amenity", map_filter(col("tags"),(k, v) => (v.isin("hospital","doctors","clinic") && k.isin("amenity")))).withColumn("is_empty", size(col("medical_amenity")) <= 0)

	val nonempty_nodes_df = nodes_filtered_df.select($"id",$"type",$"tags",$"lat",$"lon").where(col("is_empty") === "false")
	nonempty_nodes_df.persist

	//join ways and nodes 
	val join_df = nonempty_ways_df.alias("a").select($"type",$"id",$"tags",$"nd").join(nonempty_nodes_df.alias("b").select($"lat",$"lon",$"b.id"), nonempty_ways_df("nd.ref") === nonempty_nodes_df("id")).select($"a.id",$"type",$"tags",$"lat",$"lon")

	// Filter out USA border
	val nodes_in_bbox_df = node_df.where((col("lon") >=  -161.75583) && (col("lon") <= -68.01197) && (col("lat") >= 19.50139) && (col("lat") <= 64.85694))

	//union the result
	val union_df = join_df.unionAll(nodes_in_bbox_df.select($"id",$"type",$"tags",$"lat",$"lon"))

	union_df.show(truncate=false)
    }
   start
  }
}
// scalastyle:on println
