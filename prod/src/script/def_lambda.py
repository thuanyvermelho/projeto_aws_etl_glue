import boto3
import json

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')

    # Nome do bucket e arquivos esperados
    bucket_name = 'data-client-raw'
    expected_files = ['upload/dados_cadastro_1.parquet', 'upload/dados_cadastro_2.csv', 'upload/dados_cadastro_3.json']

    # Lista os objetos no bucket na pasta especificada
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='upload/')
    
    # Verifica a presença dos arquivos esperados
    present_files = [obj['Key'] for obj in response.get('Contents', [])]
    if all(file in present_files for file in expected_files):
        try:
            glue_job_name = 'job_client_health'
            response = glue_client.start_job_run(JobName=glue_job_name)
            print(f"Job do Glue iniciado com sucesso: {response['JobRunId']}")
        except Exception as e:
            print(f"Erro ao iniciar o job do Glue: {str(e)}")
    else:
        print("Nem todos os arquivos esperados estão presentes.")