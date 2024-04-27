# Desafio Técnico para a Engenheiro de Dados Bix 

O desafio consiste na extração de dados de três fontes, junção num data warehouse e orquestração do processo. As fontes são:

### - Funcionários
Esta fonte é um endpoint que retornará id e o nome do funcionário. E recebe como parâmetro de url o id a ser consultado (atualmente entre 1 e 9).

### - Vendas
Esta fonte é uma tabela de um db postgres, com valor da venda, id do funcionário atribuído àquela venda, id da categoria referente àquela venda, e o id da venda.

### - Categorias
Esta fonte é um arquivo parquet armazenado no GCS com id da categoria e nome da categoria.

Minha ideia é extrair os dados das três fontes utilizando Python com algumas bibliotecas, armazenar snapshots diários numa staging area no S3, e por fim carregar num DW no BigQuery. Utilizarei Airflow rodando num container Docker localmente para orquestrar todo este processo. Por fim, irei criar um relatório no Power BI para exibir os dados de maneira didática.

A estrutura é a seguinte:

![image](https://github.com/doridgo/bix-data-engineer-challenge/assets/69277343/8cf400bc-2e62-4dd8-bb4d-e6431cd429f5)
