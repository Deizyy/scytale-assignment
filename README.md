# Scytale Data Engineer Assignment - Compliance ETL

## Overview
This project implements an end-to-end ETL pipeline using Apache Airflow and Docker to monitor GitHub Pull Request compliance. It extracts data from the `home-assistant/core` repository, analyzes it against compliance rules (Approved Review + Passing Status Checks), and stores the results for auditing.

## Project Structure
The project is organized to ensure modularity and separation of concerns:
- **`dags/`**: Contains the Airflow DAG definition (`compliance_dag.py`) which orchestrates the workflow.
- **`plugins/`**: Contains the reusable Python business logic:
  - `github_extractor.py`: Handles interaction with the GitHub API (pagination, rate limits).
  - `compliance_transformer.py`: Contains the logic for data processing and compliance validation.
- **`data/`**: A shared volume for storing raw JSON data and processed Parquet files.
- **`docker-compose.yml`**: Defines the infrastructure, services, and environment configuration.

## Prerequisites
To run this project, ensure you have the following installed on your machine:
- **Docker** & **Docker Compose**
- A **GitHub Personal Access Token** (Classic) with `public_repo` scope.

## Setup & Run Instructions

### 1. Clone the Repository
Clone this project to your local machine and navigate to the project directory.
```bash
git clone "https://github.com/Deizyy/scytale-assignment"
cd "scytale_assignment"

### 2. Configure Security (Environment Variables)
  GITHUB_TOKEN=github_token_here

### 3. Start the Environment
Initialize the Airflow services using Docker Compose. This command will also install necessary Python dependencies (pandas, pyarrow, requests) automatically. Open a terminal in the project folder and run:
```bash
  docker-compose up

### 4. Access the Airflow UI
Open your web browser and go to: http://localhost:8080
Username: airflow
Password: airflow

### 5. Trigger the Pipeline
In the Airflow UI, locate the DAG named scytale_compliance_etl.
Toggle the Unpause switch (on the left side) to make it active (blue).
Click the Play button (Actions column) and select Trigger DAG.
Click on the DAG name and go to the Graph view to watch the execution progress in real-time.

Output Files
Upon successful execution, the pipeline generates files in the data/ directory:
Raw Data: daily_prs.json - The raw extraction from GitHub.
Compliance Report: compliance_report_YYYYMMDD.parquet - The processed file containing the compliance status for each PR.
Logging & Monitoring
Task Logs: Detailed logs for each step (Extraction, Transformation, Loading) are available in the Airflow UI. Click on a task instance in the Graph view and select Logs.
System Logs: Container-level logs can be viewed in the terminal where docker-compose is running.
Alerting: The pipeline is designed to raise specific exceptions (e.g., FileNotFoundError or API errors) which appear as failed tasks (Red) in the UI.

### Design Decisions
OOP Architecture: The logic is encapsulated in Python classes (GitHubExtractor, ComplianceTransformer) within the plugins/ folder. This promotes code reuse, testability, and keeps the DAG file clean.

Parquet Storage: The output is saved in Parquet format. This columnar storage format was chosen for its high performance, compression efficiency, and schema preservation, making it ideal for downstream analytics.

Robust Error Handling: - API Rate Limits: The extractor includes time.sleep() between requests to respect GitHub's API limits.

Retries: The DAG is configured with retries=3 and retry_delay to handle transient network issues automatically.

AI Tools Usage
According to the assignment guidelines, artificial intelligence tools were used to assist with the base code for airflow process analysis systems (DAGs) and to improve the project structure documentation.

Author: Deizy Lagziel
