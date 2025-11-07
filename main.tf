locals {
  layers = toset(["bronze","silver","gold"])
}

# KMS key
resource "aws_kms_key" "this" {
  description             = "KMS for ADHD Reddit project"
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.project}-kms"
  target_key_id = aws_kms_key.this.id
}

# S3 buckets
resource "aws_s3_bucket" "layers" {
  for_each = local.layers
  bucket   = "${var.project}-${each.key}"
}

# Block public access
resource "aws_s3_bucket_public_access_block" "layers" {
  for_each                = local.layers
  bucket                  = aws_s3_bucket.layers[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versioning
resource "aws_s3_bucket_versioning" "layers" {
  for_each = local.layers
  bucket   = aws_s3_bucket.layers[each.key].id
  versioning_configuration { status = "Enabled" }
}

# Encryption with KMS
resource "aws_s3_bucket_server_side_encryption_configuration" "layers" {
  for_each = local.layers
  bucket   = aws_s3_bucket.layers[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.this.arn
    }
  }
}

# DynamoDB table for checkpoints
resource "aws_dynamodb_table" "checkpoints" {
  name         = "${var.project}-checkpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "subreddit"

  attribute {
    name = "subreddit"
    type = "S"
  }
}# ---- Lambda execution role ----
resource "aws_iam_role" "lambda_ingest_role" {
  name = "${var.project}-lambda-ingest-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_ingest_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Inline policy: S3 write (bronze), DynamoDB checkpoints, Secrets Manager, KMS decrypt
resource "aws_iam_role_policy" "lambda_ingest_policy" {
  name = "${var.project}-lambda-ingest-policy"
  role = aws_iam_role.lambda_ingest_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        "Sid": "S3WriteBronze",
        "Effect": "Allow",
        "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListBucket"],
        "Resource": [
          "${aws_s3_bucket.layers["bronze"].arn}",
          "${aws_s3_bucket.layers["bronze"].arn}/*"
        ]
      },
      {
        "Sid": "DynamoCheckpoint",
        "Effect": "Allow",
        "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
        "Resource": "${aws_dynamodb_table.checkpoints.arn}"
      },
      {
        "Sid": "ReadSecrets",
        "Effect": "Allow",
        "Action": ["secretsmanager:GetSecretValue"],
        "Resource": "*"
      },
      {
        "Sid": "KMSDecrypt",
        "Effect": "Allow",
        "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
        "Resource": "${aws_kms_key.this.arn}"
      }
    ]
  })
}

# ---- Lambda function ----
resource "aws_lambda_function" "ingest_reddit" {
  function_name = "${var.project}-ingest-reddit"
  role          = aws_iam_role.lambda_ingest_role.arn
  runtime       = "python3.11"
  handler       = "app.handler"

  filename         = "lambdas/ingest_reddit/deployment.zip"
  source_code_hash = filebase64sha256("lambdas/ingest_reddit/deployment.zip")
  timeout = 30
  memory_size = 512
  environment {
    variables = {
      BRONZE_BUCKET    = aws_s3_bucket.layers["bronze"].bucket
      CHECKPOINT_TABLE = aws_dynamodb_table.checkpoints.name
      SECRET_NAME      = "reddit/adhd/api"
    }
  }
}
