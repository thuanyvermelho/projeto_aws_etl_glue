import pytest
import warnings
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from unittest import mock
from pyspark.sql.functions import *

# Ignorar FutureWarning do pyspark
warnings.filterwarnings("ignore", category=FutureWarning, module='pyspark')

# Mock do retorno de getResolvedOptions para fornecer JOB_NAME
def mock_get_resolved_options(args, options):
    return {'JOB_NAME': 'test_job'}

@pytest.fixture(scope="module")
def spark_session():
    # Usar SparkSession.builder para garantir que apenas uma instância do SparkSession seja criada
    return SparkSession.builder \
        .appName("GlueIntegrationTest") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

@pytest.fixture(scope="module")
def glue_context(spark_session):
    # Criar GlueContext a partir do SparkSession existente
    return GlueContext(spark_session.sparkContext)

@pytest.fixture(autouse=True)
def setup_and_teardown(monkeypatch):
    # Patch `getResolvedOptions` antes da importação do módulo
    monkeypatch.setattr('awsglue.utils.getResolvedOptions', mock_get_resolved_options)
    yield

def test_pipeline(glue_context):
    # Importar o módulo após o mock para garantir que o patch está aplicado
    from dev.src.job_pipeline import (
        transform_parquet, transform_csv, transform_json,
        tratar_valores_nulos, remover_cpfs_duplicados,
        validar_qualidade_dados, validar_esquema, expected_schema
    )

    # Caminhos para os arquivos de teste (ajustar conforme necessário)
    parquet_path = "s3://data-client-raw/upload/dados_cadastro_1.parquet"
    csv_path = "s3://data-client-raw/upload/dados_cadastro_2.csv"
    json_path = "s3://data-client-raw/upload/dados_cadastro_3.json"
    
    # Obter a sessão do Spark
    spark = glue_context.spark_session
    
    # 1. Transformar e padronizar dados
    df_parquet = transform_parquet(spark, parquet_path)
    df_csv = transform_csv(spark, csv_path)
    df_json = transform_json(spark, json_path)
    
    # 2. Combinando os DataFrames transformados
    tabela_unica = df_parquet.union(df_csv).union(df_json)
    
    # 3. Cache do DataFrame combinado para otimização de performance
    tabela_unica = tabela_unica.cache()
    # Materializar o cache realizando uma ação
    tabela_unica.count()
    
    # 4. Verificar se o DataFrame foi armazenado em cache
    assert tabela_unica.is_cached, "O DataFrame não está em cache."

    # 5. Tratar valores nulos
    tabela_unica = tratar_valores_nulos(tabela_unica)
    
    # 6. Remover CPFs duplicados
    tabela_unica = remover_cpfs_duplicados(tabela_unica)
    
    # 7. Executar testes de qualidade de dados
    try:
        validar_qualidade_dados(tabela_unica)
        dados_validos = True
    except ValueError as e:
        dados_validos = False
        print(f"Erro de validação de qualidade dos dados: {e}")

    assert dados_validos, "Falha na validação de qualidade dos dados."

    # 8. Aplicar criptografia de dados sensíveis conforme LGPD
    tabela_unica = tabela_unica.withColumn("logradouro", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("bairro", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("cidade", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("cpf", repeat(lit("*"), 10))

    # 9. Adicionar coluna de chave primária usando UUID
    tabela_unica = tabela_unica.withColumn("id", expr("uuid()"))

    # 10. Reordenar as colunas para que 'id' seja a primeira coluna
    primeira_coluna = ['id']
    outras_colunas = [col for col in tabela_unica.columns if col != 'id']
    tabela_unica = tabela_unica.select(primeira_coluna + outras_colunas)

    # 11. Validar esquema final
    try:
        tabela_unica = validar_esquema(tabela_unica, expected_schema)
        esquema_valido = True
    except ValueError as e:
        esquema_valido = False
        print(f"Erro de validação de esquema dos dados: {e}")

    assert esquema_valido, "Falha na validação do esquema dos dados."

    # 12. Verificar se o DataFrame final contém registros após o processamento
    assert tabela_unica.count() > 0, "O DataFrame final está vazio após o processamento."

    # 13. Verificar se a coluna 'id' foi adicionada corretamente e está única
    ids_count = tabela_unica.select("id").distinct().count()
    total_count = tabela_unica.count()
    assert ids_count == total_count, "A coluna 'id' não é única para todos os registros."

    # 14. Verificar se os dados sensíveis foram criptografados corretamente
    assert tabela_unica.filter(tabela_unica["cpf"].rlike("[^*]")).count() == 0, "Dados de CPF não foram completamente criptografados."
    assert tabela_unica.filter(tabela_unica["logradouro"].rlike("[^*]")).count() == 0, "Dados de logradouro não foram completamente criptografados."
    assert tabela_unica.filter(tabela_unica["bairro"].rlike("[^*]")).count() == 0, "Dados de bairro não foram completamente criptografados."
    assert tabela_unica.filter(tabela_unica["cidade"].rlike("[^*]")).count() == 0, "Dados de cidade não foram completamente criptografados."
