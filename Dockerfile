# Use a imagem base do AWS Glue
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Instalar as dependências necessárias
RUN apt-get update && apt-get install -y \
    python3-pip

# Instalar pytest na versão especificada
RUN pip3 install pytest==7.2.1

# Definir o diretório de trabalho
WORKDIR /workspace

# Comando para manter o contêiner rodando
CMD ["bash"]
