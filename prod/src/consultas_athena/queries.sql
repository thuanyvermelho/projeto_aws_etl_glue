-- Número de Clientes Cadastrados por Ano
SELECT 
    YEAR(data_cadastro) AS ano_cadastro, 
    CAST(COUNT(*) AS bigint) AS quantidade
FROM 
    db_client_insurance.clientoutput
WHERE YEAR(data_cadastro) > 2020
GROUP BY 
    YEAR(data_cadastro)
ORDER BY 
    ano_cadastro;
    
--Distribuição por Convênio
SELECT convenio, CAST(COUNT(*) AS bigint) AS quantidade
FROM db_client_insurance.clientoutput
WHERE convenio <> 'SUS'
GROUP BY convenio
ORDER BY CAST(COUNT(*) AS bigint) desc

--Distribuição por Sexo
SELECT sexo, CAST(COUNT(*) AS bigint) AS quantidade
FROM db_client_insurance.clientoutput
GROUP BY sexo;

--Distribuição por 5 Maiores Estado
SELECT estado, CAST(COUNT(*) AS bigint) AS quantidade
FROM db_client_insurance.clientoutput
GROUP BY estado
ORDER BY CAST(COUNT(*) AS bigint) desc
limit 5