# 🏥 Healthcare Data Engineering Project

An end-to-end modular data pipeline for processing, transforming, and analyzing U.S. healthcare insurance data — containerized with Docker Compose.

### 🛠️ Tech Stack

- 🧪 **Apache Spark + Iceberg + JupyterLab** – data ingestion and transformation
- 📊 **Apache Superset** – BI dashboards and visual analytics
- ⏰ **Apache Airflow** – weekly ETL orchestration
- 🐘 **PostgreSQL** – gold-layer data storage
- 🧠 **Redis** – Airflow's backend broker

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

📄 [Project Report](docs/Healthcare_Project_Report.pdf)  
🎞️ [Presentation Slides](<docs/Milestone_2_(15_April_2025).pdf>)
🎞️ [Presentation Slides](<docs/Milestone_3_(06_May_2025).pdf>)
