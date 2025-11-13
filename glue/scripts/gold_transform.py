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
silver_path = "s3://krish-redditadhd-silver/adhd_posts_silver/"
gold_path   = "s3://krish-redditadhd-gold/adhd_posts_gold/"

# ---------- 1) Read cleaned Silver data ----------
# Silver is already parquet + partitioned by year/month/day
df_silver = spark.read.parquet(silver_path)

print("=== SILVER SCHEMA ===")
df_silver.printSchema()

print("=== SILVER SAMPLE ===")
df_silver.select(
    "post_id", "subreddit", "author", "title", "score",
    "num_comments", "created_at", "year", "month", "day"
).show(5, truncate=False)

# ---------- 2) Helper: contains_any(col_name, [list of substrings]) ----------
def contains_any(col_name, keywords):
    """
    Build a big OR condition: col LIKE '%kw1%' OR col LIKE '%kw2%' ...
    Works on the lowercased columns (title_lower / body_lower).
    """
    col = F.col(col_name)
    expr = None
    for kw in keywords:
        cond = col.contains(kw)
        expr = cond if expr is None else (expr | cond)
    # If somehow no keywords, return False
    return expr if expr is not None else F.lit(False)

# ---------- 3) Engagement banding ----------
# Intuition:
#   - low  : score <= 1
#   - mid  : 2–10
#   - high : > 10
df_gold = df_silver.withColumn(
    "engagement_level",
    F.when(F.col("score") <= 1, "low")
     .when(F.col("score") <= 10, "medium")
     .otherwise("high")
)

# ---------- 4) Topic flags using simple keyword heuristics ----------
# We use the *_lower columns from Silver to make case-insensitive checks.

work_keywords = [
    "work", "job", "boss", "coworker", "coworkers",
    "office", "meeting", "deadline"
]

relationship_keywords = [
    "relationship", "partner", "girlfriend", "boyfriend",
    "wife", "husband", "spouse", "dating", "breakup"
]

productivity_keywords = [
    "focus", "procrastinate", "procrastination",
    "productive", "productivity", "motivation",
    "distracted", "can't start", "cant start", "can't finish", "cant finish"
]

medication_keywords = [
    "meds", "medication", "ritalin", "adderall",
    "vyvanse", "concerta", "stimulant", "dose", "dosage", "mg"
]

study_keywords = [
    "school", "class", "classes", "exam", "test",
    "assignment", "homework", "university", "college",
    "professor", "semester"
]

emotion_overwhelmed_keywords = ["overwhelmed", "overwhelm"]
emotion_burnout_keywords = ["burnout", "burnt out", "burned out"]
emotion_anxiety_keywords = ["anxiety", "anxious", "panic attack", "panic attacks"]

# Build a combined text column just for keyword search convenience
df_gold = df_gold.withColumn(
    "combined_lower",
    F.concat_ws(" ",
        F.col("title_lower"),
        F.col("body_lower")
    )
)

df_gold = (
    df_gold
    .withColumn(
        "is_work_related",
        contains_any("combined_lower", work_keywords)
    )
    .withColumn(
        "is_relationship_related",
        contains_any("combined_lower", relationship_keywords)
    )
    .withColumn(
        "is_productivity_related",
        contains_any("combined_lower", productivity_keywords)
    )
    .withColumn(
        "is_medication_related",
        contains_any("combined_lower", medication_keywords)
    )
    .withColumn(
        "is_study_related",
        contains_any("combined_lower", study_keywords)
    )
    .withColumn(
        "mentions_overwhelmed",
        contains_any("combined_lower", emotion_overwhelmed_keywords)
    )
    .withColumn(
        "mentions_burnout",
        contains_any("combined_lower", emotion_burnout_keywords)
    )
    .withColumn(
        "mentions_anxiety",
        contains_any("combined_lower", emotion_anxiety_keywords)
    )
)

# ---------- 5) Text for embedding / LLM ----------
# Intuition:
#   - clean_title / clean_body already have URLs stripped.
#   - We coalesce to "" so we don't get nulls.
#   - This is what you feed into embeddings / LLM later.

df_gold = df_gold.withColumn(
    "text_for_embedding",
    F.concat_ws(
        "\n\n",
        F.coalesce(F.col("clean_title"), F.lit("")),
        F.coalesce(F.col("clean_body"), F.lit(""))
    )
)

# ---------- 6) Choose final Gold columns ----------
# Keep: identifiers, time, engagement, topic flags, text_for_embedding
gold_columns = [
    "post_id",
    "subreddit",
    "author",
    "created_at",
    "year", "month", "day",
    "score",
    "num_comments",
    "post_age_days",

    "engagement_level",

    "is_work_related",
    "is_relationship_related",
    "is_productivity_related",
    "is_medication_related",
    "is_study_related",
    "mentions_overwhelmed",
    "mentions_burnout",
    "mentions_anxiety",

    "title",         # original title
    "body",          # original body
    "clean_title",
    "clean_body",
    "text_for_embedding",
]

# Filter to only columns that actually exist (defensive)
gold_columns_existing = [c for c in gold_columns if c in df_gold.columns]

df_gold_final = df_gold.select(*gold_columns_existing)

print("=== GOLD PREVIEW ===")
df_gold_final.show(10, truncate=False)

# ---------- 7) Write Gold as Parquet ----------
writer = df_gold_final.write.mode("overwrite")

partition_cols = [c for c in ["year", "month", "day"] if c in df_gold_final.columns]
if partition_cols:
    writer = writer.partitionBy(*partition_cols)

writer.parquet(gold_path)

print(f"Written gold data to {gold_path}")

job.commit()
