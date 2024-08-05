Dicionário de Dados
Este documento descreve as colunas do dataset utilizado no projeto ETL da HealthTech.

Tabela: dados_cadastro
Colunas
id

Descrição: Identificador único do registro.
Tipo de Dados: String (UUID)
Exemplo: 123e4567-e89b-12d3-a456-426614174000
cpf

Descrição: CPF do cliente.
Tipo de Dados: String
Exemplo: 123456789101
nome_completo

Descrição: Nome completo do cliente.
Tipo de Dados: String
Exemplo: João da Silva
convenio

Descrição: Tipo de convênio do cliente.
Tipo de Dados: String
Exemplo: Plano A
data_nascimento

Descrição: Data de nascimento do cliente.
Tipo de Dados: Date
Formato: yyyy-MM-dd
Exemplo: 1985-05-20
sexo

Descrição: Sexo do cliente.
Tipo de Dados: String
Exemplo: M ou F
logradouro

Descrição: Endereço do cliente.
Tipo de Dados: String
Exemplo: Rua das Flores
numero

Descrição: Número do endereço.
Tipo de Dados: Integer
Exemplo: 123
bairro

Descrição: Bairro do cliente.
Tipo de Dados: String
Exemplo: Centro
cidade

Descrição: Cidade do cliente.
Tipo de Dados: String
Exemplo: Sao Paulo
estado

Descrição: Estado do cliente.
Tipo de Dados: String
Exemplo: SP
data_cadastro

Descrição: Data de cadastro do cliente.
Tipo de Dados: Timestamp
Formato: yyyy-MM-dd'T'HH:mm:ss
Exemplo: 2023-01-15T10:30:00
Observação Importante:
Para garantir a conformidade com as melhores práticas da Lei Geral de Proteção de Dados (LGPD), os dados sensíveis foram criptografados durante os testes. Esta medida visa proteger a privacidade e segurança das informações tratadas.