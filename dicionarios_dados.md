# Dicionário de Dados 

Este documento descreve as colunas do dataset utilizado no projeto ETL.

## Tabela: dados_cadastro

### Colunas

1. **id**
   - **Descrição**: Identificador único do registro.
   - **Tipo de Dados**: String (UUID)
   - **Exemplo**: `123e4567-e89b-12d3-a456-426614174000`

2. **cpf**
   - **Descrição**: CPF do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `123456789101`

3. **nome_completo**
   - **Descrição**: Nome completo do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `João da Silva`

4. **convenio**
   - **Descrição**: Tipo de convênio do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `Plano A`

5. **data_nascimento**
   - **Descrição**: Data de nascimento do cliente.
   - **Tipo de Dados**: Date
   - **Formato**: `yyyy-MM-dd`
   - **Exemplo**: `1985-05-20`

6. **sexo**
   - **Descrição**: Sexo do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `M` ou `F`

7. **logradouro**
   - **Descrição**: Endereço do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `Rua das Flores`

8. **numero**
   - **Descrição**: Número do endereço.
   - **Tipo de Dados**: Integer
   - **Exemplo**: `123`

9. **bairro**
   - **Descrição**: Bairro do cliente.
   - **Tipo de Dados**: String
   - **Exemplo**: `Centro`

10. **cidade**
    - **Descrição**: Cidade do cliente.
    - **Tipo de Dados**: String
    - **Exemplo**: `Sao Paulo`

11. **estado**
    - **Descrição**: Estado do cliente.
    - **Tipo de Dados**: String
    - **Exemplo**: `SP`

12. **data_cadastro**
    - **Descrição**: Data de cadastro do cliente.
    - **Tipo de Dados**: Timestamp
    - **Formato**: `yyyy-MM-dd'T'HH:mm:ss`
    - **Exemplo**: `2023-01-15T10:30:00`
