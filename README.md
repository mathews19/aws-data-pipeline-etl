# AWS Data Pipeline ETL

## 🎯 Visão Geral
Pipeline de dados serverless na AWS que processa dados de vendas em tempo real, aplicando transformações e carregando em data warehouse para análise.

## 🏗️ Arquitetura

```
S3 Raw Data → Lambda Trigger → Glue ETL Job → S3 Processed → 
Step Functions Orchestration → Athena/Redshift
```

### Componentes:
- **S3**: Data Lake (raw/processed/curated layers)
- **Lambda**: Event triggers e processamento leve
- **Glue**: ETL jobs e Data Catalog
- **Step Functions**: Orquestração do pipeline
- **CloudWatch**: Monitoramento e logs
- **SNS**: Notificações de sucesso/falha

## 📁 Estrutura do Projeto

```
aws-data-pipeline-etl/
├── lambda/
│   ├── trigger_pipeline.py
│   └── data_validator.py
├── glue/
│   ├── etl_transform_sales.py
│   └── job_config.json
├── step_functions/
│   └── pipeline_state_machine.json
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── tests/
│   └── test_transformations.py
├── docs/
│   ├── architecture.png
│   └── data_flow.md
└── README.md
```

## 🚀 Features

✅ **Ingestão Automática**: Detecta novos arquivos no S3 e inicia pipeline  
✅ **Transformações**: Limpeza, agregação e enriquecimento de dados  
✅ **Orquestração**: Step Functions gerencia todo o workflow  
✅ **Qualidade de Dados**: Validações automáticas em cada etapa  
✅ **Monitoramento**: Métricas e alertas no CloudWatch  
✅ **Idempotência**: Pipeline pode ser re-executado sem duplicação  

## 🛠️ Tecnologias

- Python 3.11
- AWS Lambda
- AWS Glue (PySpark)
- AWS Step Functions
- AWS S3
- Terraform (IaC)
- Boto3

## 📊 Exemplo de Transformação

**Input (Raw):**
```json
{
  "sale_id": "S12345",
  "product": "Laptop",
  "amount": "1500.00",
  "date": "2025-01-15"
}
```

**Output (Processed):**
```json
{
  "sale_id": "S12345",
  "product_name": "Laptop",
  "product_category": "Electronics",
  "amount": 1500.00,
  "amount_brl": 7500.00,
  "sale_date": "2025-01-15",
  "year": 2025,
  "month": 1,
  "quarter": "Q1",
  "processed_at": "2025-01-15T10:30:00Z"
}
```

## 🚀 Deploy

### Pré-requisitos
```bash
# AWS CLI configurado
aws configure

# Terraform instalado
terraform --version

# Python 3.11+
python --version
```

### Passo a Passo

1. **Clone o repositório**
```bash
git clone https://github.com/mathews19/aws-data-pipeline-etl.git
cd aws-data-pipeline-etl
```

2. **Configure variáveis**
```bash
cd terraform
cp variables.tfvars.example variables.tfvars
# Edite variables.tfvars com suas configurações
```

3. **Deploy infraestrutura**
```bash
terraform init
terraform plan -var-file=variables.tfvars
terraform apply -var-file=variables.tfvars
```

4. **Upload código Lambda**
```bash
cd ../lambda
zip -r lambda_function.zip .
aws lambda update-function-code \
  --function-name data-pipeline-trigger \
  --zip-file fileb://lambda_function.zip
```

5. **Deploy Glue Job**
```bash
cd ../glue
aws s3 cp etl_transform_sales.py s3://seu-bucket/scripts/
aws glue create-job --cli-input-json file://job_config.json
```

6. **Teste o pipeline**
```bash
# Upload arquivo de teste
aws s3 cp sample_data.json s3://seu-bucket/raw/sales/
# Pipeline será acionado automaticamente
```

## 📈 Monitoramento

### CloudWatch Dashboards
- Taxa de sucesso/falha do pipeline
- Tempo de processamento médio
- Volume de dados processados
- Custos por execução

### Logs
```bash
# Ver logs do Lambda
aws logs tail /aws/lambda/data-pipeline-trigger --follow

# Ver logs do Glue
aws logs tail /aws-glue/jobs/output --follow
```

## 🧪 Testes

```bash
# Instalar dependências
pip install -r requirements-dev.txt

# Executar testes unitários
pytest tests/ -v

# Testes de integração
pytest tests/integration/ -v --aws-profile=dev
```

## 💰 Estimativa de Custos

Para 1000 arquivos/dia:
- **Lambda**: ~$5/mês
- **Glue**: ~$50/mês
- **S3**: ~$10/mês
- **Step Functions**: ~$2/mês
- **Total**: ~$67/mês

## 🔒 Segurança

- ✅ Buckets S3 com criptografia habilitada
- ✅ IAM roles com princípio do menor privilégio
- ✅ Logs de auditoria no CloudTrail
- ✅ Dados sensíveis mascarados

## 📚 Documentação Adicional

- [Diagrama de Arquitetura](docs/architecture.png)
- [Fluxo de Dados Detalhado](docs/data_flow.md)
- [Guia de Troubleshooting](docs/troubleshooting.md)

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Add nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📝 License

MIT License - veja [LICENSE](LICENSE) para detalhes.

## 👤 Autor

**Matheus Passos Gomes**
- LinkedIn: [matheus passos gomes](https://www.linkedin.com/in/matheus-passos-gomes/)
- Email: matheus.steps@gmail.com

---

⭐ Se este projeto te ajudou, considere dar uma estrela!