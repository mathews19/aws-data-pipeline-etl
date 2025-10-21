# ========================================
# AWS Data Pipeline - Terraform Configuration for LOCALSTACK
# ========================================


terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ========== PROVIDER PARA LOCALSTACK ==========

provider "aws" {
  region = var.aws_region
  
  # Credenciais fake - LocalStack não valida
  access_key = "test"
  secret_key = "test"
  
  # IMPORTANTE: Aponta para LocalStack ao invés da AWS real
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  
  # Endpoints do LocalStack
  endpoints {
    s3             = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    iam            = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    logs           = "http://localhost:4566"
    sns            = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    apigateway     = "http://localhost:4566"
    # Glue não é bem suportado no LocalStack free tier
    # glue           = "http://localhost:4566"
  }
}

# ========== LOCALS ==========

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    LocalStack  = "true"
  }
}

# ========== S3 BUCKETS ==========

# Bucket para dados RAW
resource "aws_s3_bucket" "raw_data" {
  bucket = "${local.name_prefix}-raw-data"
  
  # LocalStack não suporta force_destroy nas versões antigas
  # mas é útil para desenvolvimento
  force_destroy = true
  
  tags = merge(local.common_tags, {
    Name    = "Raw Data Bucket"
    Purpose = "Store incoming raw data files"
  })
}

# No LocalStack, configurações de bucket são simplificadas
# Versionamento
resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket para dados PROCESSADOS
resource "aws_s3_bucket" "processed_data" {
  bucket = "${local.name_prefix}-processed-data"
  
  force_destroy = true
  
  tags = merge(local.common_tags, {
    Name    = "Processed Data Bucket"
    Purpose = "Store processed data"
  })
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Bucket para scripts
resource "aws_s3_bucket" "scripts" {
  bucket = "${local.name_prefix}-scripts"
  
  force_destroy = true
  
  tags = merge(local.common_tags, {
    Name    = "Scripts Bucket"
    Purpose = "Store Lambda and Glue scripts"
  })
}

# ========== DYNAMODB TABLE ==========

resource "aws_dynamodb_table" "pipeline_executions" {
  name         = "${local.name_prefix}-executions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "execution_id"
  range_key    = "timestamp"
  
  attribute {
    name = "execution_id"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "S"
  }
  
  attribute {
    name = "status"
    type = "S"
  }
  
  # Global Secondary Index
  global_secondary_index {
    name            = "status-index"
    hash_key        = "status"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  tags = merge(local.common_tags, {
    Name = "Pipeline Executions Table"
  })
}

# ========== IAM ROLES ==========

# IAM Role para Lambda
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
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.pipeline_executions.arn,
          "${aws_dynamodb_table.pipeline_executions.arn}/index/*"
        ]
      }
    ]
  })
}

# Policy para Lambda logs (simplificada para LocalStack)
resource "aws_iam_role_policy" "lambda_logs_policy" {
  name = "lambda-logs-access"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ========== LAMBDA FUNCTION ==========

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.name_prefix}-trigger"
  retention_in_days = 7
  
  tags = local.common_tags
}

# Lambda Function
# NOTA: Você precisa criar o lambda_function.zip primeiro
resource "aws_lambda_function" "trigger_pipeline" {
  # Para este exemplo, vamos criar um ZIP dummy
  # Na prática, você usaria seu código real
  filename         = "lambda_function.zip"
  function_name    = "${local.name_prefix}-trigger"
  role            = aws_iam_role.lambda_role.arn
  handler         = "trigger_pipeline.lambda_handler"
  source_code_hash = fileexists("lambda_function.zip") ? filebase64sha256("lambda_function.zip") : ""
  runtime         = "python3.11"
  timeout         = 60
  memory_size     = 256
  
  environment {
    variables = {
      EXECUTIONS_TABLE     = aws_dynamodb_table.pipeline_executions.name
      RAW_BUCKET           = aws_s3_bucket.raw_data.id
      PROCESSED_BUCKET     = aws_s3_bucket.processed_data.id
      ENVIRONMENT          = var.environment
      # Adiciona flag para saber que está no LocalStack
      IS_LOCALSTACK        = "true"
      DYNAMODB_ENDPOINT    = "http://localhost:4566"
      S3_ENDPOINT          = "http://localhost:4566"
    }
  }
  
  tags = local.common_tags
  
  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy.lambda_s3_policy,
    aws_iam_role_policy.lambda_dynamodb_policy
  ]
}

# Permissão para S3 invocar Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_pipeline.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_data.arn
}

# S3 Bucket Notification
# NOTA: No LocalStack pode não funcionar perfeitamente
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

# ========== SNS TOPIC (OPCIONAL) ==========

resource "aws_sns_topic" "pipeline_notifications" {
  name = "${local.name_prefix}-notifications"
  
  tags = local.common_tags
}

# ========== SQS QUEUE (OPCIONAL - Para testes) ==========

resource "aws_sqs_queue" "pipeline_queue" {
  name = "${local.name_prefix}-queue"
  
  # Configurações simplificadas para LocalStack
  delay_seconds             = 0
  max_message_size          = 262144  # 256 KB
  message_retention_seconds = 86400   # 1 dia
  receive_wait_time_seconds = 0
  
  tags = local.common_tags
}

# ========== OUTPUTS ==========

output "raw_bucket_name" {
  description = "Nome do bucket de dados raw"
  value       = aws_s3_bucket.raw_data.id
}

output "raw_bucket_arn" {
  description = "ARN do bucket raw"
  value       = aws_s3_bucket.raw_data.arn
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

output "lambda_function_arn" {
  description = "ARN da função Lambda"
  value       = aws_lambda_function.trigger_pipeline.arn
}

output "dynamodb_table_name" {
  description = "Nome da tabela DynamoDB"
  value       = aws_dynamodb_table.pipeline_executions.name
}

output "sns_topic_arn" {
  description = "ARN do tópico SNS"
  value       = aws_sns_topic.pipeline_notifications.arn
}

output "sqs_queue_url" {
  description = "URL da fila SQS"
  value       = aws_sqs_queue.pipeline_queue.url
}

# ========== COMANDOS ÚTEIS ==========
# 
# Após terraform apply, você pode testar:
#
# 1. Listar buckets:
#    awslocal s3 ls
#
# 2. Listar tabelas DynamoDB:
#    awslocal dynamodb list-tables
#
# 3. Listar funções Lambda:
#    awslocal lambda list-functions
#
# 4. Ver detalhes da Lambda:
#    awslocal lambda get-function --function-name $(terraform output -raw lambda_function_name)
#
# 5. Invocar Lambda diretamente:
#    awslocal lambda invoke --function-name $(terraform output -raw lambda_function_name) output.json
#
# 6. Upload arquivo para S3:
#    echo '{"test": "data"}' > test.json
#    awslocal s3 cp test.json s3://$(terraform output -raw raw_bucket_name)/raw/sales/test.json
#
# ========================================