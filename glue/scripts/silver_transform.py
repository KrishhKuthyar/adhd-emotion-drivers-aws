import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ---------- Glue boilerplate ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------- Paths ----------
bronze_path = "s3://krishh-redditadhd-bronze/subreddit=ADHD/"
silver_path = "s3://krishh-redditadhd-silver/adhd_posts_silver/"

# ---------- 1) Read raw data from Bronze ----------
df_raw = spark.read.json(bronze_path)

print("=== RAW SCHEMA ===")
df_raw.printSchema()

print("=== RAW SAMPLE ===")
df_raw.show(5, truncate=False)

# ---------- 2) Select useful fields ----------
# We'll start with a safe subset; adjust after we see actual schema in logs.
columns_map = {
    "id": "post_id",
    "subreddit": "subreddit",
    "author": "author",
    "title": "title",
    "selftext": "body",
    "score": "score",
    "num_comments": "num_comments",
    "created_utc": "created_utc",
    "url": "url"
}

# Only keep columns that actually exist in df_raw
available = [c for c in columns_map.keys() if c in df_raw.columns]

df_sel = df_raw.select(
    *[F.col(c).alias(columns_map[c]) for c in available]
)

# ---------- 3) Add clean timestamp + basic metadata ----------
if "created_utc" in df_raw.columns:
    df_sel = df_sel.withColumn(
        "created_at",
        F.from_unixtime(F.col("created_utc")).cast("timestamp")
    )

# Derive simple partitions
if "created_at" in df_sel.columns:
    df_sel = (
        df_sel
        .withColumn("year", F.year("created_at"))
        .withColumn("month", F.month("created_at"))
        .withColumn("day", F.dayofmonth("created_at"))
    )

print("=== SILVER PREVIEW ===")
df_sel.show(5, truncate=False)

# ---------- 4) Write to Silver as Parquet ----------
(
    df_sel
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(silver_path)
)

print(f"Written silver data to {silver_path}")

job.commit()
