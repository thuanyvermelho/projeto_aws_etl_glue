# Ambiente de Desenvolvimento e Testes
No ambiente de testes unitário e de integração utiliza `pytest` para garantir a qualidade e a integridade dos dados processados. O objetivo é simular o ambiente de produção o mais próximo possível, usando PySpark e AWS Glue.

## Configuração de Testes Automatizados
Os testes são definidos para verificar cada etapa do pipeline de dados, desde a leitura e transformação dos dados até a validação de qualidade e conformidade com regulamentos como a LGPD.

[Script teste unitário ](https://github.com/thuanyvermelho/projeto_aws_etl_glue/blob/master/dev/tests/test_unit.py)<br>
[Script teste de integração ](https://github.com/thuanyvermelho/projeto_aws_etl_glue/blob/master/dev/tests/test_integration.py)

## Estrutura dos Testes
### Configuração do Ambiente de Testes:

Utilizei a biblioteca `pytest` para gerenciar os testes e `unittest.mock` para criar mocks dos métodos `getResolvedOptions` do AWS Glue, facilitando a execução de testes em um ambiente controlado.<br>
A função `spark_session` inicializa uma instância do `SparkSession` para permitir o processamento distribuído dos dados durante os testes.<br>
O `GlueContext` é configurado a partir do SparkSession para simular o ambiente de execução do AWS Glue.

## Execução dos Testes:

Os dados são lidos de arquivos de exemplo no formato Parquet, CSV e JSON armazenados no S3, representando diferentes fontes de dados.<br>
**Transformações e Padronizações:** Funções personalizadas como `transform_parquet`, <`transform_csv` e `transform_json` são testadas para garantir que os dados sejam transformados corretamente.<br>
**Otimização com Cache:** O DataFrame combinado é armazenado em cache para otimizar a performance durante os testes subsequentes, verificando se o cache é efetivamente utilizado.

### Validação de Dados:

**Tratamento de Valores Nulos:** Funções são testadas para assegurar que valores nulos sejam tratados adequadamente.<br>
**Remoção de Duplicatas:** Testes confirmam que CPFs duplicados são corretamente removidos, mantendo apenas os registros mais recentes.<br>
**Qualidade e Conformidade dos Dados:** Inclui testes de qualidade de dados para verificar a integridade, verificar a conformidade com esquemas esperados e assegurar a criptografia de dados sensíveis.<br>

### Verificações de Segurança e Conformidade:

**Criptografia de Dados Sensíveis:** Os testes garantem que dados sensíveis, como CPF, logradouro, bairro e cidade, sejam devidamente criptografados, conforme os requisitos da LGPD.

### Validação do Esquema de Dados:

Os testes validam que o esquema final dos dados processados está conforme o esperado, assegurando que todas as transformações mantêm a integridade do esquema.
#### Exemplo de Execução de Testes
Para executar os testes, use o comando abaixo:

```
pytest dev/tests/test_unit.py
pytest dev/tests/test_integration.py 
```
Isso vai verificar todas as funções importantes do pipeline de dados e apontar qualquer problema. Esses testes ajudam a garantir que o sistema esteja funcionando bem e que qualquer erro seja detectado cedo.

![alt text](imagens/testes.png)

## Benefícios da Abordagem Customizada

**Flexibilidade:** Fazer nossos próprios testes com pytest nos dá controle total para validar tudo sem precisar de outras bibliotecas.<br>
**Performance:** Usar PySpark nos testes nos permite simular um ambiente real, o que é ótimo para testar como o sistema se comporta em produção.




