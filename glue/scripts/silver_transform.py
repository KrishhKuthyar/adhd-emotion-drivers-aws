import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ===============================
# STEP 1: READ FROM BRONZE BUCKET
# ===============================
bronze_path = "s3a://krish-redditadhd-bronze/subreddit=ADHD/"
df = spark.read.json(bronze_path)

print(f"Loaded {df.count()} Reddit posts")

# show a few columns to understand structure
df.printSchema()
df.show(5, truncate=False)

#job.commit()