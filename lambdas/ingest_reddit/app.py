import os, json, time
import boto3
import praw
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
ddb = boto3.client("dynamodb")

BRONZE_BUCKET   = os.environ["BRONZE_BUCKET"]
CHECKPOINT_TABLE= os.environ["CHECKPOINT_TABLE"]
SECRET_NAME     = os.environ["SECRET_NAME"]

def get_reddit_client():
    resp = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(resp["SecretString"])
    return praw.Reddit(
        client_id=secret["client_id"],
        client_secret=secret["client_secret"],
        user_agent=secret["user_agent"]
    )

def get_checkpoint(subreddit):
    try:
        resp = ddb.get_item(
            TableName=CHECKPOINT_TABLE,
            Key={"subreddit": {"S": subreddit}}
        )
        if "Item" in resp and "last_created_utc" in resp["Item"]:
            return int(resp["Item"]["last_created_utc"]["N"])
    except ClientError:
        pass
    return 0

def set_checkpoint(subreddit, ts):
    ddb.put_item(
        TableName=CHECKPOINT_TABLE,
        Item={
            "subreddit": {"S": subreddit},
            "last_created_utc": {"N": str(ts)}
        }
    )

def handler(event, context):
    subreddit = "ADHD"
    last_ts = get_checkpoint(subreddit)
    reddit = get_reddit_client()

    posts = []
    for p in reddit.subreddit(subreddit).new(limit=500):
        if int(p.created_utc) <= last_ts:
            continue
        posts.append({
            "id": p.id,
            "created_utc": int(p.created_utc),
            "title": p.title,
            "selftext": p.selftext,
            "score": p.score,
            "num_comments": p.num_comments,
            "author": str(p.author) if p.author else None,
            "permalink": f"https://reddit.com{p.permalink}",
            "subreddit": subreddit,
            "ingested_at": int(time.time())
        })

    if not posts:
        return {"message": "No new posts"}

    # write NDJSON to Bronze, partitioned by time
    dt = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    key = f"subreddit={subreddit}/dt={dt}/batch_{int(time.time())}.json"
    body = "\n".join(json.dumps(p) for p in posts).encode("utf-8")
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body)

    set_checkpoint(subreddit, max(p["created_utc"] for p in posts))
    return {"saved": len(posts), "s3_key": key}
