
data "aws_caller_identity" "current" {}
resource "aws_iam_role" "glue_role" {
  name = "${var.project}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "${var.project}-glue-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.project}-bronze",
          "arn:aws:s3:::${var.project}-bronze/*",
          "arn:aws:s3:::${var.project}-silver",
          "arn:aws:s3:::${var.project}-silver/*"
        ]
      },
      {
        Sid    = "GlueInternalAssetsAccess"
        Effect = "Allow"
        Action = [
            "s3:GetObject",
            "s3:ListBucket"
        ]
        Resource = [
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}",
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${var.region}/*"
        ]
        },
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.this.arn
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}
