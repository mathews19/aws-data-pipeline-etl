"""
Glue ETL Job - etl_transform_sales.py
Processa dados de vendas com PySpark no AWS Glue

Este script:
1. Lê dados brutos do S3 (formato JSON)
2. Aplica transformações e enriquecimentos
3. Valida qualidade dos dados
4. Escreve dados processados em formato Parquet particionado
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ========== INICIALIZAÇÃO ==========

# Argumentos passados para o job via Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'SOURCE_KEY',
    'TARGET_BUCKET',
    'CURRENCY_CONVERSION_RATE'
])

# Contextos do Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurações
SOURCE_PATH = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
TARGET_PATH = f"s3://{args['TARGET_BUCKET']}/processed/sales/"
USD_TO_BRL = float(args.get('CURRENCY_CONVERSION_RATE', 5.0))

print("=" * 60)
print("AWS GLUE ETL JOB - Sales Data Processing")
print("=" * 60)
print(f"Job Name: {args['JOB_NAME']}")
print(f"Source: {SOURCE_PATH}")
print(f"Target: {TARGET_PATH}")
print(f"USD to BRL Rate: {USD_TO_BRL}")
print("=" * 60)


# ========== FUNÇÃO 1: LEITURA DE DADOS ==========

def read_raw_data():
    """
    Lê dados brutos do S3 em formato JSON

    Returns:
        DataFrame: Dados brutos validados contra schema
    """
    print("\n[1/4] 📥 Reading raw data from S3...")

    # Schema para validação
    # Isso garante que os dados estão no formato esperado
    schema = StructType([
        StructField("sale_id", StringType(), False),      # NOT NULL
        StructField("product", StringType(), False),       # NOT NULL
        # NOT NULL (será convertido)
        StructField("amount", StringType(), False),
        StructField("date", StringType(), False),          # NOT NULL
        StructField("customer_id", StringType(), True),    # NULLABLE
        StructField("region", StringType(), True)          # NULLABLE
    ])

    # Ler JSON do S3 com schema validation
    df = spark.read \
        .schema(schema) \
        .json(SOURCE_PATH)

    record_count = df.count()
    print(f"Successfully read {record_count:,} records")

    # Mostrar sample dos dados
    print("\nSample data (first 3 rows):")
    df.show(3, truncate=False)

    return df


# ========== FUNÇÃO 2: TRANSFORMAÇÕES ==========

def apply_transformations(df):
    """
    Aplica transformações e enriquecimentos nos dados

    Transformações aplicadas:
    1. Conversão de tipos de dados
    2. Extração de componentes de data
    3. Conversão de moeda
    4. Categorização de produtos
    5. Classificação de valor
    6. Ranking de vendas
    7. Estatísticas agregadas
    8. Flags de análise
    9. Metadados de processamento

    Args:
        df: DataFrame com dados brutos

    Returns:
        DataFrame: Dados transformados
    """
    print("\n[2/4] Applying transformations...")

    # 1. CONVERSÃO DE TIPOS
    print("  → Converting data types...")
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(10, 2)))

    # 2. EXTRAÇÃO DE COMPONENTES DE DATA
    print("  → Extracting date components...")
    df = df.withColumn("sale_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    df = df.withColumn("year", F.year("sale_date"))
    df = df.withColumn("month", F.month("sale_date"))
    df = df.withColumn("quarter", F.concat(F.lit("Q"), F.quarter("sale_date")))
    df = df.withColumn("day_of_week", F.date_format("sale_date", "EEEE"))
    df = df.withColumn("week_of_year", F.weekofyear("sale_date"))

    # 3. CONVERSÃO DE MOEDA (USD → BRL)
    print(f"  → Converting currency (USD to BRL @ {USD_TO_BRL})...")
    df = df.withColumn("amount_usd", F.col("amount"))
    df = df.withColumn("amount_brl", F.col("amount") * F.lit(USD_TO_BRL))

    # 4. CATEGORIZAÇÃO DE PRODUTOS
    print("  → Categorizing products...")
    # Dicionário de categorias
    product_categories = {
        'Laptop': 'Electronics',
        'Phone': 'Electronics',
        'Tablet': 'Electronics',
        'Monitor': 'Electronics',
        'Headphones': 'Accessories',
        'Mouse': 'Accessories',
        'Keyboard': 'Accessories',
        'Cable': 'Accessories',
        'Desk': 'Furniture',
        'Chair': 'Furniture',
        'Lamp': 'Furniture'
    }

    # Criar expressão case/when
    category_expr = F.lit("Other")  # Default
    for product, category in product_categories.items():
        category_expr = F.when(F.col("product") == product,
                               category).otherwise(category_expr)

    df = df.withColumn("product_category", category_expr)

    # 5. CLASSIFICAÇÃO DE VALOR (value tiers)
    print("  → Classifying value tiers...")
    df = df.withColumn(
        "value_tier",
        F.when(F.col("amount") >= 1000, "High")
         .when(F.col("amount") >= 500, "Medium")
         .when(F.col("amount") >= 100, "Low")
         .otherwise("Very Low")
    )

    # 6. RANKING DE VENDAS POR PRODUTO
    print("  → Calculating product rankings...")
    window_product = Window.partitionBy("product").orderBy(F.desc("amount"))
    df = df.withColumn("product_sale_rank",
                       F.row_number().over(window_product))

    # 7. ESTATÍSTICAS AGREGADAS POR PRODUTO
    print("  → Computing aggregate statistics...")
    product_stats = df.groupBy("product").agg(
        F.avg("amount").alias("product_avg_amount"),
        F.max("amount").alias("product_max_amount"),
        F.min("amount").alias("product_min_amount"),
        F.count("*").alias("product_total_sales")
    )

    # Join com estatísticas
    df = df.join(product_stats, "product", "left")

    # 8. FLAGS DE ANÁLISE
    print("  → Creating analysis flags...")

    # Flag: venda acima da média do produto
    df = df.withColumn(
        "above_product_average",
        F.when(F.col("amount") > F.col(
            "product_avg_amount"), True).otherwise(False)
    )

    # Flag: venda premium (top 20% do produto)
    df = df.withColumn(
        "is_premium_sale",
        F.when(F.col("amount") >= F.col("product_max_amount")
               * 0.8, True).otherwise(False)
    )

    # Flag: fim de semana
    df = df.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin(
            ["Saturday", "Sunday"]), True).otherwise(False)
    )

    # 9. METADADOS DE PROCESSAMENTO
    print("  → Adding processing metadata...")
    df = df.withColumn("processed_at", F.current_timestamp())
    df = df.withColumn("processing_date", F.current_date())
    df = df.withColumn("source_file", F.lit(SOURCE_PATH))
    df = df.withColumn("etl_version", F.lit("1.0.0"))

    # Remover coluna temporária
    df = df.drop("date")

    print("Transformations completed")
    print(f"   Total columns: {len(df.columns)}")

    return df


# ========== FUNÇÃO 3: VALIDAÇÃO DE QUALIDADE ==========

def apply_data_quality_checks(df):
    """
    Aplica validações de qualidade dos dados

    Validações:
    1. Remove duplicatas
    2. Remove registros com nulls em campos obrigatórios
    3. Remove registros com valores inválidos
    4. Adiciona score de qualidade

    Args:
        df: DataFrame com dados transformados

    Returns:
        DataFrame: Dados validados
    """
    print("\n[3/4] Applying data quality checks...")

    initial_count = df.count()
    print(f"  Initial record count: {initial_count:,}")

    # 1. REMOVER DUPLICATAS (baseado em sale_id)
    print(" Removing duplicates...")
    df_dedup = df.dropDuplicates(["sale_id"])
    duplicates_removed = initial_count - df_dedup.count()
    if duplicates_removed > 0:
        print(f"   Removed {duplicates_removed:,} duplicate records")
    else:
        print(f"=== No duplicates found ===")

    # 2. VALIDAR CAMPOS OBRIGATÓRIOS
    print("  → Validating required fields...")
    df_valid = df_dedup.filter(
        F.col("sale_id").isNotNull() &
        F.col("product").isNotNull() &
        F.col("amount").isNotNull() &
        F.col("sale_date").isNotNull()
    )

    nulls_removed = df_dedup.count() - df_valid.count()
    if nulls_removed > 0:
        print(
            f" Removed {nulls_removed:,} records with null required fields")
    else:
        print(f" All required fields populated")

    # 3. VALIDAR VALORES
    print(" Validating value ranges...")
    df_clean = df_valid.filter(
        # Amount must be positive
        (F.col("amount") > 0) &
        (F.col("sale_date") <= F.current_date()) &       # Date can't be in future
        (F.col("sale_date") >= F.lit("2020-01-01"))      # Reasonable date range
    )

    invalid_removed = df_valid.count() - df_clean.count()
    if invalid_removed > 0:
        print(
            f" Removed {invalid_removed:,} records with invalid values")
    else:
        print(f"    ✓ All values within valid ranges")

    # 4. CALCULAR SCORE DE QUALIDADE (0-100)
    print(" Calculating data quality score...")
    df_clean = df_clean.withColumn("data_quality_score", F.lit(100))

    # Penalizar falta de customer_id (-10 pontos)
    df_clean = df_clean.withColumn(
        "data_quality_score",
        F.when(F.col("customer_id").isNull(),
               F.col("data_quality_score") - 10)
        .otherwise(F.col("data_quality_score"))
    )

    # Penalizar falta de region (-10 pontos)
    df_clean = df_clean.withColumn(
        "data_quality_score",
        F.when(F.col("region").isNull(),
               F.col("data_quality_score") - 10)
        .otherwise(F.col("data_quality_score"))
    )

    # Penalizar valores extremos (-20 pontos se amount > 10000)
    df_clean = df_clean.withColumn(
        "data_quality_score",
        F.when(F.col("amount") > 10000,
               F.col("data_quality_score") - 20)
        .otherwise(F.col("data_quality_score"))
    )

    # 5. RESUMO DA QUALIDADE
    final_count = df_clean.count()
    retention_rate = (final_count / initial_count *
                      100) if initial_count > 0 else 0

    print(f"\n  Quality Summary:")
    print(f"    Initial records:  {initial_count:,}")
    print(f"    Final records:    {final_count:,}")
    print(f"    Retention rate:   {retention_rate:.2f}%")
    print(f"    Records removed:  {initial_count - final_count:,}")

    # Distribuição de quality score
    quality_distribution = df_clean.groupBy(
        "data_quality_score").count().orderBy("data_quality_score")
    print(f"\n  Quality Score Distribution:")
    quality_distribution.show()

    print(" Data quality checks completed")

    return df_clean


# ========== FUNÇÃO 4: ESCRITA DE DADOS ==========

def write_processed_data(df):
    """
    Escreve dados processados no S3 em formato Parquet

    Features:
    - Formato Parquet (compressão Snappy)
    - Particionamento por year/month
    - Modo append (adiciona dados sem sobrescrever)

    Args:
        df: DataFrame com dados processados e validados
    """
    print("\n[4/4] Writing processed data to S3...")
    print(f"  Target path: {TARGET_PATH}")
    print(f"  Format: Parquet (Snappy compression)")
    print(f"  Partitioning: year/month")

    # Escrever em Parquet particionado
    df.write \
        .mode("append") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(TARGET_PATH)

    print(" Data written successfully")

    # ESTATÍSTICAS FINAIS
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)

    # Agregações por categoria
    category_stats = df.groupBy("product_category").agg(
        F.count("*").alias("total_records"),
        F.sum("amount_usd").alias("total_amount_usd"),
        F.avg("amount_usd").alias("avg_amount_usd"),
        F.min("amount_usd").alias("min_amount_usd"),
        F.max("amount_usd").alias("max_amount_usd")
    ).orderBy(F.desc("total_amount_usd"))

    print("\nSales by Product Category:")
    category_stats.show(truncate=False)

    # Top 10 vendas
    print("\nTop 10 Largest Sales:")
    df.select(
        "sale_id",
        "product",
        "amount_usd",
        "sale_date",
        "value_tier"
    ).orderBy(F.desc("amount_usd")).show(10, truncate=False)

    # Estatísticas gerais
    total_sales = df.agg(F.sum("amount_usd")).collect()[0][0]
    avg_sale = df.agg(F.avg("amount_usd")).collect()[0][0]
    total_records = df.count()

    print("\nGeneral Statistics:")
    print(f"  Total Records:     {total_records:,}")
    print(f"  Total Sales (USD): ${total_sales:,.2f}")
    print(f"  Average Sale:      ${avg_sale:,.2f}")

    print("\n" + "=" * 60)


# ========== FUNÇÃO PRINCIPAL ==========

def main():
    """
    Função principal que orquestra todo o pipeline ETL

    Pipeline:
    1. Read raw data from S3
    2. Apply transformations
    3. Apply data quality checks
    4. Write processed data to S3
    """
    start_time = datetime.now()

    try:
        print("\n🚀 Starting ETL Pipeline...")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Pipeline ETL
        raw_df = read_raw_data()
        transformed_df = apply_transformations(raw_df)
        clean_df = apply_data_quality_checks(transformed_df)
        write_processed_data(clean_df)

        # Tempo total
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Start time:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration:    {duration:.2f} seconds")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print("ETL PIPELINE FAILED")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("=" * 60)
        raise e


# ========== EXECUÇÃO ==========

if __name__ == "__main__":
    main()
    job.commit()  # Finaliza o job no Glue
