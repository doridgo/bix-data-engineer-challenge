# Desafio Técnico para a Engenheiro de Dados Bix 

O desafio consiste na extração de dados de três fontes, junção num data warehouse e orquestração do processo. As fontes são:

### - Funcionários
Esta fonte é um endpoint que retornará id e o nome do funcionário. E recebe como parâmetro de url o id a ser consultado (atualmente entre 1 e 9).

### - Vendas
Esta fonte é uma tabela de um db postgres, com valor da venda, id do funcionário atribuído àquela venda, id da categoria referente àquela venda, e o id da venda.

### - Categorias
Esta fonte é um arquivo parquet armazenado no GCS com id da categoria e nome da categoria.

Minha ideia é extrair os dados das três fontes utilizando Python com algumas bibliotecas (que estão no requirements.txt), armazenar snapshots diários numa staging area no S3, e por fim carregar num DW no BigQuery. Utilizarei Airflow rodando num container Docker localmente para orquestrar todo este processo. Por fim, irei criar um relatório no Power BI para exibir os dados de maneira didática.

A estrutura é a seguinte:

![image](https://github.com/doridgo/bix-data-engineer-challenge/assets/69277343/8cf400bc-2e62-4dd8-bb4d-e6431cd429f5)

Inicio estabelecendo a conexão com as clouds que serão utilizadas (GCP e AWS), importando de um arquivo credentials.py que também está no repositório.

Depois, defino as funções que serão utilizadas na DAG que orquestrará todo o processo.

### extract_api_employees()
Esta função tem o objetivo de acessar o endpoint https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior que recebe como parâmetro um id que atualmente varia de 1 a 9. Para caso o número de funcionários aumente ou diminua, a função realizará um loop para puxar todos os funcionários, independente de quantos sejam. O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de funcionarios.
Utilizo **requests** para extrair o dado do endpoint, **pandas** para criar o dataframe e transformar para csv, **boto3** para estabelecer a conexão com a AWS e criar os objetos nas pastas organizadas no S3.

### extract_sql_sales()
Esta função tem como objetivo extrair os dados da tabela de vendas de um banco de dado Postgres. O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de vendas. 
Utilizo **sqlalchemy** para criar a engine que conectará com o postgres, **pandas** para criar o dataframe e transformar para csv, **boto3** para estabelecer a conexão com a AWS e criar os objetos nas pastas organizadas no S3.

### extract_parquet_categories()
Esta função tem como objetivo extrair os dados de categoria que estão num arquivo parquet. O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de categorias.
Utilizo **pandas** para ler o arquivo parquet e realizar as transformações, e **boto3** para estabelecer a conexão com a AWS e criar os objetos nas pastas organizadas no S3.

### join_dfs()
Esta função tem o objetivo de juntar os arquivos gerados pelas tasks anteriores desta dag, apagando colunas desnecessárias. O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de tabelas juntas.
Utilizo **boto3** para ler e carregar os arquivos do S3, **pandas** para gerar, unir os dataframes e transformar os dados.

### load_df_bq()
Esta função tem o objetivo de carregar a tabela que foi resultado da junção das três tabelas de vendas, categorias e funcionários, num armazém de dados no Big Query. O resultado é a atualização da tabela bix_sales no Big Query.
Utilizo **boto3** para ler o arquivo csv das tabelas unidas e **pandas_gbq** para carregar para o Big Query.

Nas configurações da DAG, estabeleço que as funções de **extração** ocorram simultaneamente, depois ocorra a junção e a carga. Ela está configurada para rodar todos os dias, às 11 horas da manhã (horário de Brasília).
