# ========================================
# AWS Data Pipeline - Terraform Configuration
# ========================================
# 
# Este arquivo define toda a infraestrutura AWS necessária para o projeto:
# - S3 Buckets (raw, processed, scripts)
# - DynamoDB Table
# - IAM Roles e Policies
# - Lambda Function
# - Glue Job
# - CloudWatch Logs
#
# Como usar:
#   terraform init
#   terraform plan
#   terraform apply
# ========================================

# ========== TERRAFORM CONFIGURATION ==========

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # Opcional: Backend para armazenar state remotamente
  # Descomente quando quiser usar S3 para state
  # backend "s3" {
  #   bucket = "seu-bucket-terraform-state"
  #   key    = "data-pipeline/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

# ========== PROVIDER CONFIGURATION ==========

provider "aws" {
  region = var.aws_region
  
  # Tags padrão aplicadas a todos os recursos
  default_tags {
    tags = {
      Project     = "AWS-Data-Pipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = var.owner
    }
  }
}

# ========== DATA SOURCES ==========

# Obter account ID atual
data "aws_caller_identity" "current" {}

# Obter região atual
data "aws_region" "current" {}

# ========== LOCALS ==========

locals {
  # Prefixo para nomear recursos
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Account ID
  account_id = data.aws_caller_identity.current.account_id
  
  # Região
  region = data.aws_region.current.name
  
  # Tags comuns
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ========== S3 BUCKETS ==========

# Bucket para dados RAW (entrada)
resource "aws_s3_bucket" "raw_data" {
  bucket = "${local.name_prefix}-raw-data"
  
  tags = merge(local.common_tags, {
    Name = "Raw Data Bucket"
    Purpose = "Store incoming raw data files"
  })
}

# Versionamento do bucket raw
resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Criptografia do bucket raw
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Bloqueio de acesso público
resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket para dados PROCESSADOS (saída)
resource "aws_s3_bucket" "processed_data" {
  bucket = "${local.name_prefix}-processed-data"
  
  tags = merge(local.common_tags, {
    Name = "Processed Data Bucket"
    Purpose = "Store processed data in Parquet format"
  })
}

# Configurações do bucket processed (mesmas do raw)
resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket para scripts (Lambda e Glue)
resource "aws_s3_bucket" "scripts" {
  bucket = "${local.name_prefix}-scripts"
  
  tags = merge(local.common_tags, {
    Name = "Scripts Bucket"
    Purpose = "Store Lambda and Glue scripts"
  })
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# ========== DYNAMODB TABLE ==========

# Tabela para controle de execuções
resource "aws_dynamodb_table" "pipeline_executions" {
  name           = "${local.name_prefix}-executions"
  billing_mode   = "PAY_PER_REQUEST"  # On-demand, sem provisionamento
  hash_key       = "execution_id"
  range_key      = "timestamp"
  
  attribute {
    name = "execution_id"
    type = "S"  # String
  }
  
  attribute {
    name = "timestamp"
    type = "S"  # String (ISO format)
  }
  
  attribute {
    name = "status"
    type = "S"
  }
  
  # Index secundário para buscar por status
  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  # Point-in-time recovery
  point_in_time_recovery {
    enabled = true
  }
  
  tags = merge(local.common_tags, {
    Name = "Pipeline Executions Table"
  })
}

# ========== IAM ROLES ==========

# Role para Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${local.name_prefix}-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# Policy para Lambda acessar S3
resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "lambda-s3-access"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      }
    ]
  })
}

# Policy para Lambda acessar DynamoDB
resource "aws_iam_role_policy" "lambda_dynamodb_policy" {
  name = "lambda-dynamodb-access"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query"
        ]
        Resource = [
          aws_dynamodb_table.pipeline_executions.arn,
          "${aws_dynamodb_table.pipeline_executions.arn}/index/*"
        ]
      }
    ]
  })
}

# Policy para Lambda escrever logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Role para Glue
resource "aws_iam_role" "glue_role" {
  name = "${local.name_prefix}-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# Policy gerenciada para Glue
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Policy customizada para Glue acessar S3
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      }
    ]
  })
}

# ========== LAMBDA FUNCTION ==========

# CloudWatch Log Group para Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.name_prefix}-trigger"
  retention_in_days = 7
  
  tags = local.common_tags
}

# Lambda Function
resource "aws_lambda_function" "trigger_pipeline" {
  filename         = "lambda_function.zip"  # Você vai criar este ZIP
  function_name    = "${local.name_prefix}-trigger"
  role            = aws_iam_role.lambda_role.arn
  handler         = "trigger_pipeline.lambda_handler"
  source_code_hash = filebase64sha256("lambda_function.zip")
  runtime         = "python3.11"
  timeout         = 60
  memory_size     = 256
  
  environment {
    variables = {
      GLUE_JOB_NAME        = aws_glue_job.transform_sales.name
      EXECUTIONS_TABLE     = aws_dynamodb_table.pipeline_executions.name
      PROCESSED_BUCKET     = aws_s3_bucket.processed_data.id
      ENVIRONMENT          = var.environment
    }
  }
  
  tags = local.common_tags
  
  depends_on = [aws_cloudwatch_log_group.lambda_logs]
}

# Permissão para S3 invocar Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_pipeline.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_data.arn
}

# Notificação S3 para Lambda
resource "aws_s3_bucket_notification" "raw_data_notification" {
  bucket = aws_s3_bucket.raw_data.id
  
  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_pipeline.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/sales/"
    filter_suffix       = ".json"
  }
  
  depends_on = [aws_lambda_permission.allow_s3]
}

# ========== GLUE JOB ==========

# CloudWatch Log Group para Glue
resource "aws_cloudwatch_log_group" "glue_logs" {
  name              = "/aws-glue/jobs/${local.name_prefix}-transform-sales"
  retention_in_days = 7
  
  tags = local.common_tags
}

# Glue Job
resource "aws_glue_job" "transform_sales" {
  name     = "${local.name_prefix}-transform-sales"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue/etl_transform_sales.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/temp/"
    "--SOURCE_BUCKET"                    = aws_s3_bucket.raw_data.id
    "--TARGET_BUCKET"                    = aws_s3_bucket.processed_data.id
    "--CURRENCY_CONVERSION_RATE"         = "5.0"
  }
  
  # Configuração de recursos
  glue_version      = "4.0"  # Versão mais recente do Glue
  max_retries       = 1
  timeout           = 60  # minutos
  number_of_workers = 2
  worker_type       = "G.1X"  # 1 DPU por worker
  
  tags = local.common_tags
}

# ========== GLUE CATALOG DATABASE ==========

resource "aws_glue_catalog_database" "sales_db" {
  name        = "${local.name_prefix}_sales_db"
  description = "Database for sales data"
  
  location_uri = "s3://${aws_s3_bucket.processed_data.bucket}/sales/"
}

# ========== SNS TOPIC (OPCIONAL) ==========

# Tópico SNS para notificações
resource "aws_sns_topic" "pipeline_notifications" {
  name = "${local.name_prefix}-notifications"
  
  tags = local.common_tags
}

# Subscription de email (você precisa confirmar manualmente)
resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_notifications.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# ========== OUTPUTS ==========

output "raw_bucket_name" {
  description = "Nome do bucket de dados raw"
  value       = aws_s3_bucket.raw_data.id
}

output "processed_bucket_name" {
  description = "Nome do bucket de dados processados"
  value       = aws_s3_bucket.processed_data.id
}

output "scripts_bucket_name" {
  description = "Nome do bucket de scripts"
  value       = aws_s3_bucket.scripts.id
}

output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = aws_lambda_function.trigger_pipeline.function_name
}

output "glue_job_name" {
  description = "Nome do Glue job"
  value       = aws_glue_job.transform_sales.name
}

output "dynamodb_table_name" {
  description = "Nome da tabela DynamoDB"
  value       = aws_dynamodb_table.pipeline_executions.name
}

output "glue_database_name" {
  description = "Nome do banco de dados no Glue Catalog"
  value       = aws_glue_catalog_database.sales_db.name
}

output "sns_topic_arn" {
  description = "ARN do tópico SNS"
  value       = aws_sns_topic.pipeline_notifications.arn
}