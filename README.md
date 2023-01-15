### Ingest data to Google Cloud Storage and Google BigQuery with Apache Airflow on Docker.

##### Clone this repository and enter the directory
```bash
git clone https://github.com/isa96/airflow-docker.git && cd airflow-gcloud-docker
```

##### Modify the DAG according to your needs
```bash
nano dags/data_ingestion.py
```

##### Create Airflow stacks with Docker Compose
```bash
sudo docker compose up -d
```

##### Open Airflow with username "airflow" and password "airflow" then run the DAG
```bash
localhost:8080
```
