# ========================================
# Variables Configuration
# ========================================

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "data-pipeline"
}

variable "environment" {
  description = "Ambiente"
  type        = string
  default     = "local"
}

variable "aws_region" {
  description = "Região AWS (fake para LocalStack)"
  type        = string
  default     = "us-east-1"
}