# Breweries ETL Pipeline

## Overview

This repository contains an ETL pipeline that extracts brewery data from the Open Brewery DB API, processes it through bronze, silver, and gold layers, and stores results in MinIO using Apache Airflow.

## Layers

- **Bronze:** Raw JSON data from API.
- **Silver:** Parquet files partitioned by state.
- **Gold:** Parquet file with aggregated brewery counts by state and type.

## Requirements

- Docker

## How to Run

1. **Install Docker**  
   Download and install Docker: [https://www.docker.com/get-started/](https://www.docker.com/get-started/)

2. **Clone the repository**
    ```bash
    git clone https://github.com/yourusername/extract_data_breweries.git
    cd extract_data_breweries
    ```

3. **Initialize services**
    ```bash
    make init
    make up
    ```

4. **Access Airflow**
    - Open your browser and go to [http://localhost:8080](http://localhost:8080)
    - Username: `airflow`
    - Password: `airflow`

5. **Access MinIO**
    - Open your browser and go to [http://localhost:9001](http://localhost:9001)
    - Username: `minioadmin`
    - Password: `minioadmin`

6. **Turn on the DAG**
    - In the Airflow webserver, enable the DAG `ETL-breweries` to start

## Monitoring

- Task failures and data quality checks are logged and can trigger alerts via Airflow.
