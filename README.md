# Health Insurance Data Engineering Pipeline 

An end-to-end data engineering pipeline for processing, transforming, and analyzing healthcare insurance data. Built using Apache Spark, Apache Iceberg, PostgreSQL, Apache Airflow, and Apache Superset — fully containerized with Docker Compose.


# 🏥 Healthcare Data Engineering Project

This project builds a modular data engineering workflow using Docker Compose, integrating:

- 🧪 **Apache Spark + Iceberg + Jupyter Lab** – for data transformation
- 📊 **Apache Superset** – for BI dashboards and visual analytics
- ⏰ **Apache Airflow** – for scheduling weekly ETL jobs
- 🐘 **PostgreSQL** – for storing gold-layer data
- 🧠 **Redis** – for Airflow's broker backend

---

## 📁 Folder Structure

```bash
.
├── Dockerfile                    # Custom Spark + Iceberg + Jupyter image
├── docker-compose.yml           # Multi-container setup
├── notebooks/                   # Your Jupyter notebooks (Bronze → Gold)
├── iceberg_warehouse/           # Iceberg table warehouse
├── airflow/
│   ├── dags/                    # Airflow DAGs (ETL scheduling)
│   ├── logs/
│   └── plugins/
└── README.md
```

## In Terminal

```bash
git clone https://github.com/<your-org-or-username>/practicum_project.git
cd practicum_project

docker compose up --build
```
