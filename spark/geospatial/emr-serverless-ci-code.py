from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import geopandas as gpd
from pyspark.sql.types import StringType
from shapely.geometry import Point
from bokeh.io.export import get_screenshot_as_png
from bokeh.io.webdriver import create_chromium_webdriver
from bokeh.models import ColorBar, GeoJSONDataSource, LinearColorMapper
from bokeh.palettes import Reds9 as palette
from bokeh.plotting import figure
import io
import boto3
import datetime
import sys

spark = SparkSession\
        .builder\
        .appName("Custom Images with EMR Serverless")\
        .enableHiveSupport()\
        .getOrCreate()

print(len(sys.argv))
if (len(sys.argv) != 3):
        print("Usage: emr-serverless-ci.py [bucketname] [prefixname]")
        sys.exit(0)

bucket = sys.argv[1]
prefix = sys.argv[2]
import datetime 
now = datetime.datetime.now()
ts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
key = f"{prefix}/{ts}.png"

# Read state and county files from US census data downloaded to the image
STATE_FILE = "file:///usr/local/share/bokeh/cb_2020_us_state_500k.zip"
COUNTY_FILE = "file:///usr/local/share/bokeh/cb_2020_us_county_500k.zip"
        
# Read data from OpenStreetMap
planet_df = spark.read.orc("s3://osm-pds/planet/planet-latest.orc")
planet_history_df = spark.read.orc("s3://osm-pds/planet-history/history-latest.orc")
changesets_df = spark.read.orc("s3://osm-pds/changesets/changesets-latest.orc")

# Select relevant columns from nodes
node_df = planet_df.select("id","type","tags","lat","lon").where(F.col("type") == "node")

# Select relevant columns from ways
ways_df = planet_df.select("id", "type", "tags","nds").where(F.col("type") == "way")

ways_filtered_df = ways_df.select("*", F.map_filter(
    "tags",
    lambda k, v: v.isin("hospital","doctors","clinic") & k.isin("amenity"))
    .alias("medical_amenity")
).withColumn("is_empty", F.size(F.col("medical_amenity")) <= 0)

nonempty_ways_df = ways_filtered_df.select("id", "type", "tags", "medical_amenity", "is_empty", F.explode("nds").alias("nd")).where(F.col("is_empty") == "false")
nonempty_ways_df.persist()

# Filter out USA states 
nodes_in_bbox_df = node_df.select("*").where((F.col("lon") >=  -161.75583) & (F.col("lon") <= -68.01197) & (F.col("lat") >= 19.50139) & (F.col("lat") <= 64.85694))

nodes_filtered_df = node_df.select("*", F.map_filter(
    "tags",
    lambda k, v: v.isin("hospital","doctors","clinic") & k.isin("amenity"))
    .alias("medical_amenity")
).withColumn("is_empty", F.size(F.col("medical_amenity")) <= 0)

nonempty_nodes_df = nodes_filtered_df.select("id","type","tags","lat","lon").where(F.col("is_empty") == "false")
nonempty_nodes_df.persist()

join_df = nonempty_ways_df.alias("a").select("type","id","tags","nd").join(nonempty_nodes_df.alias("b").select("lat","lon","b.id"), nonempty_ways_df["nd.ref"] == nonempty_nodes_df["id"]).select("a.id","type","tags","lat","lon")

union_df = join_df.unionAll(nodes_in_bbox_df.select("id","type","tags","lat","lon")).limit(10000)

columns_to_cast = ["lat","lon"]
union_df_temp = (
   union_df
   .select(
     *(c for c in union_df.columns if c not in columns_to_cast),
     *(F.col(c).cast("float").alias(c) for c in columns_to_cast)
   )
)

# Get US state and county details 

county_df = gpd.read_file(COUNTY_FILE)
state_df = gpd.read_file(STATE_FILE)

bc_county = spark.sparkContext.broadcast(dict(zip(county_df["GEOID"], county_df["geometry"])))

def find_first_county_id(longitude: float, latitude: float):
    from shapely.geometry import Point 
    p = Point(longitude, latitude)
    for index, geo in bc_county.value.items():
        if geo.intersects(p):
            return index
    return None


find_first_county_id_udf_v2 = F.udf(find_first_county_id, StringType())

county_df_tomerge = union_df_temp.withColumn(
        "GEOID",
        find_first_county_id_udf_v2(
            union_df_temp.lon, union_df_temp.lat
        ),
    )

hospital_count_by_county = (
        county_df_tomerge.groupBy("GEOID")
        .agg({"id": "count"})
        .withColumnRenamed("count(id)", "number_of_hospitals")
    )

#county_df.show(truncate=False)

county_hospital_df = county_df.merge(hospital_count_by_county.toPandas(), on="GEOID")
color_column = "number_of_hospitals"

state_json = state_df.to_crs("ESRI:102003").to_json()
county_json = county_hospital_df.to_crs("ESRI:102003").to_json()

# Building the plot
reversed_palette = tuple(reversed(palette))
p = figure(
        title="Geomap for US hospitals, clinic and doctors",
        plot_width=1100,
        plot_height=700,
        toolbar_location=None,
        x_axis_location=None,
        y_axis_location=None,
        tooltips=[
            ("County", "@NAME"),
            ("Hospitals", "@number_of_hospitals"),
        ],
    )
color_mapper = LinearColorMapper(palette=reversed_palette)
p.grid.grid_line_color = None
p.hover.point_policy = "follow_mouse"
p.patches(
        "xs",
        "ys",
        fill_alpha=0.0,
        line_color="black",
        line_width=0.5,
        source=GeoJSONDataSource(geojson=state_json),
    )
p.patches(
        "xs",
        "ys",
        fill_alpha=0.7,
        fill_color={"field": color_column, "transform": color_mapper},
        line_color="black",
        line_width=0.5,
        source=GeoJSONDataSource(geojson=county_json),
    )

color_bar = ColorBar(color_mapper=color_mapper, label_standoff=12, width=10)
p.add_layout(color_bar, "right")

driver = create_chromium_webdriver(["--no-sandbox"])

file = get_screenshot_as_png(p, height=700, width=1100, driver=driver)

#Upload the image to S3 

print(f"Uploading image data to s3://{bucket}/{key}")
client = boto3.client("s3")
in_mem_file = io.BytesIO()
file.save(in_mem_file, format="png")
in_mem_file.seek(0)
client.put_object(Bucket=bucket, Key=key, Body=in_mem_file)
