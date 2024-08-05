import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from datetime import date, datetime
from unittest import mock
import sys

class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mock_get_resolved_options = mock.patch(
            'awsglue.utils.getResolvedOptions',
            return_value={'JOB_NAME': 'test_job'}
        ).start()

        cls.mock_job = mock.patch('awsglue.job.Job').start()
        cls.mock_init = cls.mock_job.return_value.init
        cls.mock_commit = cls.mock_job.return_value.commit

        global tratar_valores_nulos, remover_cpfs_duplicados, validar_esquema
        from dev.src.job_pipeline import (
            tratar_valores_nulos, remover_cpfs_duplicados, validar_esquema
        )

        cls.spark = SparkSession.builder \
            .master("local") \
            .appName("TestFunctions") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        mock.patch.stopall()

    def test_tratar_valores_nulos(self):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [("1", None, 25), ("2", "John", None)]
        df = self.spark.createDataFrame(data, schema)

        df_tratado = tratar_valores_nulos(df)
        expected_data = [("1", "N/A", 25), ("2", "John", 0)]
        expected_df = self.spark.createDataFrame(expected_data, schema)

        self.assertEqual(df_tratado.collect(), expected_df.collect())

    def test_remover_cpfs_duplicados(self):
        schema = StructType([
            StructField("cpf", StringType(), False),
            StructField("name", StringType(), False),
            StructField("data_cadastro", StringType(), False)
        ])
        data = [("12345678901", "John Doe", "2022-01-01"),
                ("12345678901", "John Doe", "2022-02-01"),
                ("98765432100", "Jane Doe", "2022-03-01")]
        df = self.spark.createDataFrame(data, schema)

        df_sem_duplicatas = remover_cpfs_duplicados(df)
        self.assertEqual(df_sem_duplicatas.count(), 2)

    def test_validar_esquema(self):
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

        df_valido = self.spark.createDataFrame(
            [("1", "12345678901", "Paulinho da Viola", "SUS", date(2000, 1, 1), "M", "Rua A", 123, "Vila Pescadinho", "Diadema", "SP", datetime(2022, 1, 1, 0, 0))],
            expected_schema
        )

        # # Validar DataFrame válido
        # df_valido_result = validar_esquema(df_valido, expected_schema)
        # self.assertEqual(df_valido_result.schema, expected_schema)

        # # Esquema errado intencionalmente para teste
        # esquema_errado = StructType([
        #     StructField("id", StringType(), False),
        #     StructField("cpf", IntegerType(), False)  # Tipo incorreto intencionalmente
        # ])

        # df_invalido = self.spark.createDataFrame([("1", 12345678901)], esquema_errado)

        # with self.assertRaises(ValueError):
        #     validar_esquema(df_invalido, expected_schema)

if __name__ == "__main__":
    unittest.main()
