<a name="readme-top"></a>

[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![LinkedIn][linkedin-shield]][linkedin-url]

# Airflow-ETL-AdventureWorks
## 1. About This Project
This project is an ETL (Extract, Transform, Load) pipeline. The pipeline integrates multiple technologies, including Apache Airflow, Spark, Hadoop, and Hive, to orchestrate and schedule workflow, ensure efficient data processing, storage.

**Purpose**: Automate data ingestion and transformation, providing a structured data repository for business intelligence and analysis.

## 2. Technologies Used
- **Apache Airflow**: Orchestrates and schedules the ETL workflows.
- **Apache Spark**: Processes and transforms data within the Hadoop Distributed File System (HDFS).
- **Hadoop (HDFS)**: Stores data in a scalable, distributed system.
- **Apache Hive**: Acts as the data warehouse layer, storing transformed data for querying and analysis.
- **Docker**: Manages containerized environments for each component.
## 3. Project Structure
```
📦 Airflow-ETL-AdventureWorks
├─ Makefile
├─ README.md
├─ docker-compose.yml
├─ containers
│  ├─ airflow
│  │  ├─ Dockerfile
│  │  ├─ airflow.env
│  │  └─ requirements.txt
│  ├─ hadoop
│  │  └─ hadoop.env
│  ├─ hive
│  │  ├─ hdfs-site.xml
│  │  └─ hive-site.xml
│  └─ spark
│     ├─ spark-master.env
│     └─ spark-worker.env
├─ data
├─ logs
│  ├─ airflow
│  └─ hiveserve
├─ script
│  ├─ create_repo_dirs.sh
│  └─ download_data.sh
└─ src
   ├─ config
   │  └─ config_services.py
   ├─ dags
   │  ├─ etl-adventureworks.py
   │  └─ hql
   │     └─ create_hive_tbls.hql
   ├─ jobs
   │  ├─ load_dim_dates.py
   │  ├─ load_fct_sales.py
   │  ├─ load_stg_dim_customer.py
   │  ├─ load_stg_dim_employee.py
   │  ├─ load_stg_dim_geography.py
   │  ├─ load_stg_dim_product.py
   │  ├─ load_stg_dim_promotion.py
   │  ├─ load_stg_dim_salesterritory.py
   │  └─ load_stg_sales.py
   └─ utils
      └─ utils.py
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)
## 4. About the Pipeline
This ETL pipeline loads local data into Hadoop, processes and transforms it with Spark, and stores it in Hive following data warehouse architecture.
### Data Flow: Structured data flow from raw input to refined warehouse tables.
![framework](assets/images/Framework.png)
### Airflow DAG: Orchestrates tasks such as data ingestion, transformation, and loading into the data warehouse.
![dag](assets/images/DAG-Graph.png)
### Entity Relational Diagram
**Data Warehouse Schema:** Organized into dimension and fact tables, storing sales, customer, employee, geography, product, promotion, and sales territory data.

![erd](assets/images/Entity-Relational-Diagram-Sales-AdventureWorks.png)

# Installation
## 1. Setup
### Clone the project:
```
git clone https://github.com/Doanh-Chinh/Airflow-ETL-AdventureWorks
cd Airflow-ETL-AdventureWorks
```
### Build and start the containers:
```
make up
```
## 2. Getting Started
### Interfaces
Access each service’s UI:

- Airflow: `localhost:8080` (Username: `admin`, password: `admin`).  
- Spark: `localhost:8081`
- HDFS: `localhost:9870`
- Hiveserver: `localhost:10002`
### Query Pipeline Output in Data Warehouse
Run queries in Hive CLI within the container to access processed data in the data warehouse tables.
```
make beeline
```
## 3. Tear Down
To stop and remove all containers:
```
make down
```
<p align="right">(<a href="#readme-top">back to top</a>)</p>

# Acknowledgment
Thank [@Thong-Cao](https://github.com/Thong-Cao) for giving me a chance to complete my work.

Thank [@previous-work](https://github.com/minkminkk/etl-opensky) for providing a fantastic ETL pipeline and configuration files that helps a lot for this project.

<!-- Badges -->
[forks-shield]: https://img.shields.io/github/forks/Doanh-Chinh/Airflow-ETL-AdventureWorks.svg?style=for-the-badge
[forks-url]: https://github.com/Doanh-Chinh/Airflow-ETL-AdventureWorks/network/members
[stars-shield]: https://img.shields.io/github/stars/Doanh-Chinh/Airflow-ETL-AdventureWorks.svg?style=for-the-badge
[stars-url]: https://github.com/Doanh-Chinh/Airflow-ETL-AdventureWorks/stargazers
[linkedin-shield]: https://img.shields.io/badge/LinkedIn-Profile-blue?style=for-the-badge&logo=linkedin
[linkedin-url]: https://www.linkedin.com/in/chinh-luong-doanh/
