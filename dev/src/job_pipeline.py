# -*- coding: utf-8 -*- #
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, StructType, StructField
from pyspark.sql.window import Window
import unicodedata
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Função para remover acentos
def remove_accents(inputStr):
    if inputStr is not None:
        return unicodedata.normalize('NFD', inputStr).encode('ascii', 'ignore').decode('utf-8')
    else:
        return None

# Registrar a função como uma UDF
remove_accents_udf = udf(remove_accents, StringType())

# Mapeamento das siglas dos estados
siglas_estados = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM", "Bahia": "BA", "Ceará": "CE",
    "Distrito Federal": "DF", "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA", "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS", "Minas Gerais": "MG", "Pará": "PA", "Paraíba": "PB", "Paraná": "PR",
    "Pernambuco": "PE", "Piauí": "PI", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN", "Rio Grande do Sul": "RS",
    "Rondônia": "RO", "Roraima": "RR", "Santa Catarina": "SC", "São Paulo": "SP", "Sergipe": "SE", "Tocantins": "TO"
}

# Função UDF para substituir o nome do estado pela sigla
def substituir_estado_por_sigla(estado_nome):
    return siglas_estados.get(estado_nome, estado_nome)

substituir_estado_por_sigla_udf = udf(substituir_estado_por_sigla, StringType())

# Esquema esperado
expected_schema = StructType([
    StructField("id", StringType(), False),
    StructField("cpf", StringType(), False),
    StructField("nome_completo", StringType(), False),
    StructField("convenio", StringType(), False),
    StructField("data_nascimento", DateType(), False),
    StructField("sexo", StringType(), False),
    StructField("logradouro", StringType(), False),
    StructField("numero", IntegerType(), False),
    StructField("bairro", StringType(), False),
    StructField("cidade", StringType(), False),
    StructField("estado", StringType(), False),
    StructField("data_cadastro", TimestampType(), False)
])

# Validador de esquema e qualidade dos dados
def validar_esquema(df, expected_schema):
    actual_schema = df.schema
    for expected_field, actual_field in zip(expected_schema, actual_schema):
        if expected_field.name != actual_field.name or expected_field.dataType != actual_field.dataType:
            raise ValueError(f"Esquema não corresponde ao esperado.\nEsperado: {expected_schema}\nAtual: {actual_schema}")
    return df

# Função para tratar valores nulos
def tratar_valores_nulos(df):
    df = df.withColumn("nome_completo", when(col("nome_completo").isNull(), lit("N/A")).otherwise(col("nome_completo")))
    df = df.withColumn("convenio", when(col("convenio").isNull(), lit("N/A")).otherwise(col("convenio")))
    df = df.withColumn("numero", when(col("numero").isNull(), lit(0)).otherwise(col("numero")))
    df = df.withColumn("data_nascimento", when(col("data_nascimento").isNull(), lit("1970-01-01").cast(DateType())).otherwise(col("data_nascimento")))
    df = df.withColumn("data_cadastro", when(col("data_cadastro").isNull(), lit("1970-01-01 00:00:00").cast(TimestampType())).otherwise(col("data_cadastro")))
    return df

# Remover CPFs duplicados e manter o registro com a data de cadastro mais recente
def remover_cpfs_duplicados(df):
    window_spec = Window.partitionBy("cpf").orderBy(col("data_cadastro").desc())
    df = df.withColumn("row_number", row_number().over(window_spec)) \
           .filter(col("row_number") == 1) \
           .drop("row_number")
    return df

# Validação de dados 
def validar_qualidade_dados(df):
    # Verificar se há valores nulos nas colunas obrigatórias
    colunas_obrigatorias = ["nome_completo", "convenio"]
    colunas_com_nulos = [coluna for coluna in colunas_obrigatorias if df.filter(col(coluna).isNull() | (trim(col(coluna)) == "")).count() > 0]
    if colunas_com_nulos:
        raise ValueError(f"As seguintes colunas contêm valores nulos ou em branco: {', '.join(colunas_com_nulos)}")

    # Verificar se o DataFrame possui registros
    if df.count() == 0:
        raise ValueError("O DataFrame está vazio. Nenhum registro encontrado.")

    # Verificar se a coluna 'cpf' não é nula, tem exatamente 11 caracteres, é uma string com dígitos e é única
    df_cpf_invalido = df.filter(col("cpf").isNull() | (length(col("cpf")) != 11) | (~col("cpf").rlike("^\d{11}$")))
    if df_cpf_invalido.count() > 0:
        raise ValueError(f"Existem {df_cpf_invalido.count()} registros com CPF nulo, com formato inválido ou que não são strings com 11 dígitos.")

    # Verificar se os valores de CPF são únicos
    df_cpf_duplicado = df.groupBy("cpf").count().filter(col("count") > 1)
    if df_cpf_duplicado.count() > 0:
        raise ValueError(f"Existem {df_cpf_duplicado.count()} CPFs duplicados.")

    print("Todos os registros estão válidos.")

# Função para ler, transformar e padronizar dados do Parquet
def transform_parquet(spark, parquet_path):
    df_parquet = spark.read.parquet(parquet_path)
    df_parquet_tratamento = df_parquet.alias("df_parquet_tratamento")
    df_transformado_parquet = df_parquet_tratamento.withColumnRenamed("documento_cpf", "cpf") \
                                    .withColumn("cpf", regexp_replace(col("cpf"), "[.-]", "")) \
                                    .withColumn("nome_completo", trim(regexp_replace(regexp_replace(col("nome_completo"), "Sr\\.|Sra\\.|Dr\\.|Srta\\.|Dra\\.", ""), "\\s+", " "))) \
                                    .withColumn("data_nascimento", to_date(col("data_nascimento"), "yyyy-MM-dd")) \
                                    .withColumn("data_cadastro", to_timestamp(col("data_cadastro"), "yyyy-MM-dd'T'HH:mm:ss")) \
                                    .withColumn("cidade", initcap(col("cidade"))) \
                                    .withColumn("sexo", when(col("sexo") == "Fem", "F").otherwise(when(col("sexo") == "Masc", "M").otherwise(col("sexo")))) \
                                    .withColumn("numero", col("numero").cast(IntegerType())) \
                                    .withColumnRenamed("uf", "estado") \
                                    .drop("__index_level_0__") \
                                    .drop("pais")
    for column_name in df_transformado_parquet.schema.names:
        if isinstance(df_transformado_parquet.schema[column_name].dataType, StringType):
            df_transformado_parquet = df_transformado_parquet.withColumn(column_name, remove_accents_udf(col(column_name)))
    colunas_ordenadas = ["cpf", "nome_completo", "convenio" ,"data_nascimento", "sexo" , "logradouro" , "numero" , "bairro", "cidade" , "estado" , "data_cadastro"]
    df_transformado_parquet = df_transformado_parquet.select(colunas_ordenadas)
    return df_transformado_parquet

# Função para ler, transformar e padronizar dados do CSV
def transform_csv(spark, csv_path):
    df_csv = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(csv_path)
    df_csv_tratamento = df_csv.alias("df_csv_tratamento")
    df_transformado_csv = df_csv_tratamento.withColumn("nome", trim(regexp_replace(regexp_replace(col("nome"), "Sr\\.|Sra\\.|Dr\\.|Srta\\.|Dra\\.", ""), "\\s+", " "))) \
                                    .withColumnRenamed("nome", "nome_completo") \
                                    .withColumn("data_nascimento", to_date(col("data_nascimento"), "dd/MM/yyyy")) \
                                    .withColumn("data_nascimento", col("data_nascimento").cast("date")) \
                                    .withColumn("data_cadastro", to_timestamp(col("data_cadastro"), "yyyy-MM-dd HH:mm:ss"))\
                                    .withColumn("cidade", initcap(col("cidade"))) \
                                    .withColumn("sexo", when(col("sexo") == "Fem", "F").otherwise(when(col("sexo") == "Masc", "M").otherwise(col("sexo")))) \
                                    .withColumn("numero", regexp_replace(col("numero"), "[\\.0]", "")) \
                                    .withColumn("numero", col("numero").cast(IntegerType())) \
                                    .drop("pais_cadastro")
    for column_name in df_transformado_csv.schema.names:
        if isinstance(df_transformado_csv.schema[column_name].dataType, StringType):
            df_transformado_csv = df_transformado_csv.withColumn(column_name, remove_accents_udf(col(column_name)))
    colunas_ordenadas = ["cpf", "nome_completo", "convenio" ,"data_nascimento", "sexo" , "logradouro" , "numero" , "bairro", "cidade" , "estado" , "data_cadastro"]
    df_transformado_csv = df_transformado_csv.select(colunas_ordenadas)
    return df_transformado_csv

# Função para ler, transformar e padronizar dados do JSON
def transform_json(spark, json_path):
    df_json = spark.read.json(json_path)
    df_json_tratamento = df_json.alias("df_json_tratamento")
    df_transformado_json = df_json_tratamento\
        .withColumn("nome", trim(regexp_replace(regexp_replace(col("nome"), "Sr\\.|Sra\\.|Dr\\.|Srta\\.|Dra\\.", ""), "\\s+", " ")))\
        .withColumnRenamed("nome", "nome_completo")\
        .withColumn("cpf", regexp_replace(col("cpf"), "[.-]", "")) \
        .withColumn("data_nascimento", to_date(col("data_nascimento"), "MMMM dd, yyyy"))\
        .withColumn("data_nascimento", col("data_nascimento").cast("date"))\
        .withColumn("data_cadastro", from_unixtime(col("data_cadastro") / 1000).cast("timestamp"))\
        .withColumn("cidade", initcap(col("cidade")))\
        .withColumn("numero", col("numero").cast(IntegerType()))\
        .withColumn("estado", substituir_estado_por_sigla_udf(col("estado")))
    for column_name in df_transformado_json.schema.names:
        if isinstance(df_transformado_json.schema[column_name].dataType, StringType):
            df_transformado_json = df_transformado_json.withColumn(column_name, remove_accents_udf(col(column_name)))
    colunas_ordenadas = ["cpf", "nome_completo", "convenio" ,"data_nascimento", "sexo" , "logradouro" , "numero" , "bairro", "cidade" , "estado" , "data_cadastro"]
    df_transformado_json = df_transformado_json.select(colunas_ordenadas)
    return df_transformado_json

# Função principal para executar o ETL
def main():
    # Inicialização do contexto Glue e SparkSession
    args = getResolvedOptions(sys.argv, ['JOB_NAME','PARQUET_PATH', 'CSV_PATH', 'JSON_PATH', 'OUTPUT_PATH'])
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Caminhos dos arquivos de entrada
    parquet_path = args['PARQUET_PATH']
    csv_path = args['CSV_PATH']
    json_path = args['JSON_PATH']


    # Transformar e padronizar dados
    df_transformado_parquet = transform_parquet(spark, parquet_path)
    df_transformado_csv = transform_csv(spark, csv_path)
    df_transformado_json = transform_json(spark, json_path)

    # Combinando os DataFrames
    tabela_unica = df_transformado_parquet.union(df_transformado_csv).union(df_transformado_json)

    # Tratar valores nulos
    tabela_unica = tratar_valores_nulos(tabela_unica)

    # Remover CPFs duplicados
    tabela_unica = remover_cpfs_duplicados(tabela_unica)

    # Executar testes de qualidade de dados
    validar_qualidade_dados(tabela_unica)

    # Aplicando LGPD na tabela cadastro e criptografando os dados sensíveis
    tabela_unica = tabela_unica.withColumn("logradouro", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("bairro", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("cidade", repeat(lit("*"), 10))
    tabela_unica = tabela_unica.withColumn("cpf", repeat(lit("*"), 10))

    # Adicionar coluna de chave primária usando UUID
    tabela_unica = tabela_unica.withColumn("id", expr("uuid()"))

    # Reordenar as colunas para que 'id' seja a primeira coluna
    primeira_coluna = ['id']
    outras_colunas = [col for col in tabela_unica.columns if col != 'id']
    tabela_unica = tabela_unica.select(primeira_coluna + outras_colunas)

    # Validar esquema final
    tabela_unica = validar_esquema(tabela_unica, expected_schema)

    # Salvar o DataFrame resultante no S3 particionado por convenio
    output_path = args['OUTPUT_PATH']
    tabela_unica.write.partitionBy("convenio").parquet(output_path, mode="overwrite")

    # Finalizar o trabalho
    job.commit()

# Só execute o main() se este arquivo for executado como script principal
if __name__ == "__main__":
    main()
