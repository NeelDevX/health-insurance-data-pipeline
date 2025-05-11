# Healthcare Data Engineering Project

An end-to-end modular data pipeline for processing, transforming, and analyzing U.S. healthcare insurance data — containerized with Docker Compose.

This project aims to streamline how hospitals and insurance providers analyze healthcare costs and coverage options. By organizing and processing raw hospital data into clean, structured insights, it helps identify the best insurance plans for different treatments and hospitals. The end goal is to support data-driven decision-making that improves transparency, optimizes plan selection, and ultimately enhances patient access to affordable care.

---

### 🛠️ Tech Stack

- 🧪 **Apache Spark + Iceberg + JupyterLab** – data ingestion and transformation  
- 📊 **Apache Superset** – BI dashboards and visual analytics  
- ⏰ **Apache Airflow** – weekly ETL orchestration  
- 🐘 **PostgreSQL** – gold-layer data storage   

---

## 📁 Folder Structure

```bash
health-insurance-data-pipeline/
├── docs/
│   ├── Healthcare_Project_Report.pdf
│   └── Milestone_2_15April2025.pptx
│   └── Milestone_3_06May2025.pptx
├── Dockerfile                    # Spark + Iceberg + Jupyter base image
├── docker-compose.yml           # Multi-container orchestration
├── Code/                        # Jupyter notebooks (Bronze → Silver → Gold)
├── iceberg_warehouse/           # Iceberg table warehouse
├── airflow/
│   ├── dags/                    # Airflow DAGs for each ETL stage
│   ├── logs/                    # Airflow logs
│   └── scripts/                 # Python functions used by DAGs
└── README.md
```



## In Terminal

```bash
git clone https://github.com/NeelDevX/health-insurance-data-pipeline.git
cd health-insurance-data-pipeline

docker compose up --build
```

## Project Materials

📄 [Project Report](docs/Healthcare_Project_Report.pdf)  
🎞️ [Milestone 2 Slides](docs/Milestone_2_15April2025.pptx)  
🎞️ [Milestone 3 Slides](docs/Milestone_3_06May2025.pptx)  
