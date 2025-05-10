# 🏥 Healthcare Data Engineering Project

An end-to-end modular data pipeline for processing, transforming, and analyzing U.S. healthcare insurance data — containerized with Docker Compose.

## 🛠️ Tech Stack

- <img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" width="20"/> **Apache Spark** + <img src="https://iceberg.apache.org/img/iceberg-logo.png" width="20"/> **Iceberg** + <img src="https://upload.wikimedia.org/wikipedia/commons/3/38/Jupyter_logo.svg" width="20"/> **JupyterLab** – data ingestion and transformation
- <img src="https://superset.apache.org/images/superset-logo-horiz.png" width="20"/> **Apache Superset** – BI dashboards and visual analytics
- <img src="https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png" width="20"/> **Apache Airflow** – weekly ETL orchestration
- <img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" width="20"/> **PostgreSQL** – gold-layer data storage
- <img src="https://upload.wikimedia.org/wikipedia/en/6/6b/Redis_Logo.svg" width="20"/> **Redis** – Airflow's backend broker

---

## 📁 Folder Structure

````bash
health-insurance-data-pipeline/
├── docs/
│   ├── Healthcare_Project_Report.pdf
│   └── Presentation_Slides.pdf
├── Dockerfile                    # Spark + Iceberg + Jupyter base image
├── docker-compose.yml           # Multi-container orchestration
├── Code/                        # Jupyter notebooks (Bronze → Silver → Gold)
├── iceberg_warehouse/           # Iceberg table warehouse
├── airflow/
│   ├── dags/                    # Airflow DAGs for each ETL stage
│   ├── logs/                    # Airflow logs
│   └── scripts/                 # Python functions used by DAGs
└── README.md



## In Terminal

```bash
cd health-insurance-data-pipeline
git clone https://github.com/NeelDevX/health-insurance-data-pipeline.git

docker compose up --build
````

## Project Materials

📄 [Project Report](docs/Healthcare_Project_Report.pdf)<br>
🎞️ [Milestone 2 Slides](<docs/Milestone_2_(15_April_2025).pdf>)<br>
🎞️ [Milestone 3 Slides](<docs/Milestone_3_(06_May_2025).pdf>)
