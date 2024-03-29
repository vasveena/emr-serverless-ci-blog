// This job finds all the hospitals, doctors and clinics from Open Street Map data

// scalastyle:off println
package emrserverless.customjre

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OpenStreetMapAnalysis extends Serializable {
  def main(args: Array[String]) {
   def start() = {
        val spark = SparkSession.builder().appName("EMR Serverless Custom Java Runtime with Custom Image").enableHiveSupport().getOrCreate()
        import spark.implicits._

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

        println("Displaying hospitals, clinics and doctor offices from Open Street Map")
	union_df.show(truncate=false)
    }
   start
  }
}
// scalastyle:on println
