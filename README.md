# airflow-spark
Simple data pipeline with workflow tool, unified analytics engine and containerization

#### Clonar Projeto 1

`git clone https://github.com/cordon-thiago/airflow-spark`

#### Clonar Projeto 2

`https://github.com/matheusaltoe/airflow-spark`

#### Copiar arquivos

Copiar os scripts da pasta _dags_ do projeto 2 para a pasta _airflow-spark/dags_ do projeto 1.

Copiar os scripts da pasta _app_ do projeto 2 para a pasta _airflow-spark/spark/app_ do projeto 1.

Copiar o arquivo da pasta _data_ do projeto 2 para a pasta _airflow-spark/spark/resources/data_ do projeto 1.

Copiar e substituir o arquivo _docker-compose.yml_ do projeto 2 para a pasta _airflow-spark/docker_ do projeto 1.

Copiar e substituir o arquivo _requirements.txt_ do projeto 2 para a pasta _docker/docker-airflow_ do projeto 1.

#### Criar imagem do Airflow

`$ cd airflow-spark/docker/docker-airflow`

`$ docker build --rm -t docker-airflow-spark:latest .`

#### Iniciar containers

`$ cd airflow-spark/docker`

`$ docker-compose up -d`

#### Acessos

Spark Master:http://localhost:8181

Airflow: http://localhost:8282

