# Real-Time Fraud Detection Pipeline 🕵️‍♂️💸

![Spark](https://img.shields.io/badge/Apache_Spark-Streaming-orange?style=flat-square&logo=apachespark)
![Kafka](https://img.shields.io/badge/Confluent_Kafka-7.5-black?style=flat-square&logo=apachekafka)
![Postgres](https://img.shields.io/badge/PostgreSQL-13-blue?style=flat-square&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800?style=flat-square&logo=grafana)

## Project Overview
This project implements an **End-to-End Real-Time Data Pipeline** (Kappa Architecture) to detect and visualize fraudulent transactions as they occur. 

Unlike batch processing systems, this architecture focuses on **Low Latency, Data Consistency, and Fault Tolerance**. The system ingests high-frequency transaction streams, aggregates metrics in real-time using Spark Structured Streaming, and guarantees **Idempotency** (Exactly-Once semantics) in the database layer.

## Architecture & Workflow
The solution is designed as a modular streaming pipeline containerized with Docker:

1.  **Data Ingestion:**
    * A Python-based **Event Producer** simulates banking transactions with weighted probabilities for different categories (Retail, Food, Electronics).
    * Events are buffered into **Apache Kafka** (Topic: `transactions`) to decouple generation from processing.
2.  **Stream Processing:**
    * **Spark Structured Streaming** consumes the Kafka topic.
    * Implements **Windowed Aggregations** (Tumbling Windows of 10 min, Sliding every 1 min) to calculate fraud metrics per category.
3.  **Persistence Strategy (The Hard Part):**
    * Bypassed the standard Spark JDBC limitation (Append-only) to prevent duplicates.
    * Implemented a custom **Upsert Pattern** using Staging Tables and SQL Merge.
4.  **Observability:**
    * **Grafana** connects to PostgreSQL to visualize transaction volume and fraud trends in real-time using Time-Series panels.

## Engineering Highlights & Challenges
Instead of simple ML metrics, the success of this project is measured by architectural robustness:

| Challenge | Solution Implemented |
| :--- | :--- |
| **Data Duplication** | Spark's "At-Least-Once" guarantee can create duplicates on restart. I implemented a **Merge/Upsert strategy** ( `ON CONFLICT DO UPDATE`) in Postgres to ensure data is unique per time window. |
| **Dependency Hell** | Managed complex Java/Scala dependencies (Kafka clients, JDBC drivers) within the Docker container runtime using `--packages` and volume mounting. |
| **Time Travel** | Solved visualization artifacts in Grafana by enforcing **UTC Timezones** across the entire stack (Producer -> Docker -> DB -> Dashboard). |

### The "Golden Code" (Upsert Logic)
The core mechanism that guarantees data integrity:
```python
# Writing to Staging -> Merging to Production
sql_merge = """
INSERT INTO fraud_metrics (window_start, window_end, category, count)
SELECT window_start, window_end, category, count FROM fraud_staging_metrics
ON CONFLICT (window_start, window_end, category) 
DO UPDATE SET count = EXCLUDED.count;
"""

├── src/
│   ├── producer.py           
│   ├── detector.py            
│   └── postgresql-42.7.8.jar  
├── docker-compose.yml           
└── README.md                    
