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

# ---------- 2) Select & rename useful fields ----------
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

available = [c for c in columns_map.keys() if c in df_raw.columns]

df_silver = df_raw.select(
    *[F.col(c).alias(columns_map[c]) for c in available]
)

# ---------- 3) Add created_at timestamp + partitions ----------
if "created_utc" in df_silver.columns:
    df_silver = df_silver.withColumn(
        "created_at",
        F.from_unixtime(F.col("created_utc")).cast("timestamp")
    )

if "created_at" in df_silver.columns:
    df_silver = (
        df_silver
        .withColumn("year", F.year("created_at"))
        .withColumn("month", F.month("created_at"))
        .withColumn("day", F.dayofmonth("created_at"))
    )

# ---------- 4) Filter out junk / empty posts ----------
df_silver = df_silver.withColumn(
    "body_normalized",
    F.lower(F.coalesce(F.col("body"), F.lit("")))
)

df_silver = df_silver.filter(
    ( ~F.col("body_normalized").isin("[deleted]", "[removed]", "") ) |
    ( F.col("title").isNotNull() & (F.length(F.col("title")) > 0) )
)

df_silver = df_silver.drop("body_normalized")

# ---------- 5) Basic text & metadata features ----------
df_silver = df_silver.withColumn(
    "title_length",
    F.length(F.coalesce(F.col("title"), F.lit("")))
)

df_silver = df_silver.withColumn(
    "body_length",
    F.length(F.coalesce(F.col("body"), F.lit("")))
)

df_silver = df_silver.withColumn(
    "body_word_count",
    F.when(
        F.col("body").isNotNull(),
        F.size(F.split(F.col("body"), r"\s+"))
    ).otherwise(0)
)

df_silver = df_silver.withColumn(
    "has_body",
    (F.col("body").isNotNull()) & (F.length(F.col("body")) > 0)
)

# ---------- 6) Cleaned / normalized text ----------
url_pattern = r"http[s]?://\\S+"

df_silver = df_silver.withColumn(
    "clean_body",
    F.when(
        F.col("body").isNotNull(),
        F.regexp_replace(F.col("body"), url_pattern, "")
    )
)

df_silver = df_silver.withColumn(
    "clean_title",
    F.when(
        F.col("title").isNotNull(),
        F.regexp_replace(F.col("title"), url_pattern, "")
    )
)

df_silver = df_silver.withColumn(
    "body_lower",
    F.lower(F.coalesce(F.col("clean_body"), F.lit("")))
)

df_silver = df_silver.withColumn(
    "title_lower",
    F.lower(F.coalesce(F.col("clean_title"), F.lit("")))
)

# ---------- 7) Post age ----------
if "created_at" in df_silver.columns:
    df_silver = df_silver.withColumn(
        "post_age_days",
        F.datediff(F.current_date(), F.col("created_at"))
    )

# ---------- 8) Preview & write ----------
print("=== SILVER PREVIEW ===")
df_silver.show(5, truncate=False)

writer = df_silver.write.mode("overwrite")
partition_cols = [c for c in ["year", "month", "day"] if c in df_silver.columns]
if partition_cols:
    writer = writer.partitionBy(*partition_cols)

writer.parquet(silver_path)

print(f"Written silver data to {silver_path}")

job.commit()
