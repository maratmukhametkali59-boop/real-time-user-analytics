# Real-Time User Analytics

End-to-end Data Engineering pet project that simulates user activity, streams events through Kafka, stores them in PostgreSQL, builds analytical data marts with Airflow, and visualizes results in Metabase.

## Architecture

```text
Python Producer
       ↓
     Kafka
       ↓
Python Consumer
       ↓
 PostgreSQL
       ↓
   Airflow
       ↓
 Data Marts
       ↓
  Metabase
```

## Tech Stack

* Python 3.12
* Apache Kafka
* PostgreSQL 16
* Apache Airflow 2.8
* Metabase
* Docker & Docker Compose
* SQL

## Project Structure

```text
real-time-user-analytics/

├── producer/
│   └── kafka_producer.py

├── consumer/
│   └── consumer.py

├── airflow/
│   └── dags/
│       ├── test_dag.py
│       ├── build_daily_sales.py
│       └── build_all_marts.py

├── docker-compose.yml

└── README.md
```

## Data Flow

### 1. Producer

Generates random user activity events:

* view
* click
* add_to_cart
* purchase

Example event:

```json
{
  "user_id": 18,
  "session_id": "sess_6787",
  "product_id": 324,
  "price": 26517.20,
  "event_type": "purchase",
  "page": "/checkout",
  "event_time": "2026-06-07T02:27:57"
}
```

### 2. Kafka

Producer sends events into Kafka topic:

```text
user_events_v2
```

### 3. Consumer

Consumes events from Kafka and stores them into PostgreSQL table:

```sql
raw_events
```

### 4. Airflow

Airflow DAGs build analytical marts:

#### mart_daily_sales

Daily revenue and purchases.

| event_date | purchases | revenue   |
| ---------- | --------- | --------- |
| 2026-06-07 | 90        | 920549.95 |

#### mart_top_products

Top products by revenue.

| product_id | purchases | revenue |
| ---------- | --------- | ------- |
| 123        | 10        | 50000   |

### 5. Metabase

Dashboards and visualizations built on top of data marts.

## Airflow DAGs

### build_daily_sales

Creates daily sales mart.

### build_all_marts

Creates:

* mart_daily_sales
* mart_top_products

## Running the Project

### Start infrastructure

```bash
docker compose up -d
```

### Run Producer

```bash
python3 producer/kafka_producer.py
```

### Run Consumer

```bash
python3 consumer/consumer.py
```

### Open Airflow

```text
http://localhost:8080
```

### Open Metabase

```text
http://localhost:3000
```

## Skills Demonstrated

* Event Streaming with Kafka
* Data Ingestion
* ETL Development
* SQL Data Transformation
* Airflow Orchestration
* PostgreSQL
* Docker
* Data Mart Design
* BI Visualization
* Git & GitHub

## Future Improvements

* dbt integration
* Automated tests
* Kafka partitions and scaling
* Data quality checks
* Cloud deployment (AWS/GCP)
* CI/CD pipeline

## Author

Marat Mukhametkali

System Analyst transitioning into Data Engineering.

