# Retail Analytics Pipeline

A robust, end-to-end data analytics platform for retail transactions, built on the Medallion Architecture (Bronze → Silver → Gold). This project demonstrates modern data engineering practices using industry-standard tools for data ingestion, transformation, and visualization.

## 📦 Technology Stack

| Layer | Component | Purpose |
|-------|-----------|---------|
| **Generate Data** | Python | Script to generate data pushed to postgres |
| **Source** | PostgreSQL | Transactional retail database |
| **Ingestion** | Airbyte | Change Data Capture (CDC) & replication |
| **Transformation** | DBT | SQL-based data transformations & testing |
| **Orchestration** | Dagster | Workflow automation & job scheduling |
| **Storage** | ClickHouse | Columnar analytics database |
| **Visualization** | Apache Superset | Business intelligence dashboards |
| **Monitoring** | Grafana | Pipeline monitoring & alerting |

## 🏗️ Architecture Layers

### Bronze Layer (Raw Data)
- PostgreSQL source contains raw retail transactions
- Data includes: customer_id, product_category, item_count, purchase_amount, payment_method, store_location, transaction_timestamp
- Airbyte captures changes via CDC and replicates to ClickHouse

### Silver Layer (Cleaning & Validation)
- DBT models clean and validate raw data
- Data quality checks ensure referential integrity
- Deduplication removes duplicate transactions
- Transformations run on a scheduled basis via Dagster

### Gold Layer (Analytics Ready)
- Aggregated fact tables (daily/weekly sales, customer metrics)
- Dimension tables (customers, products, stores)
- Pre-computed metrics for fast dashboarding
- ClickHouse optimizes query performance

### Visualization & Monitoring
- **Superset:** Business users create dashboards and reports
- **Grafana:** Operations team monitors pipeline health and data quality

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** (v3.8+)
- **Python 3.11+** (for local script execution)
- **Git**
- **4GB RAM minimum** (8GB recommended)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/OlawaleOluwafemi/retail-analytics-pipeline.git
   cd retail-analytics-pipeline
   ```

2. **Set up environment variables:**
   Create a `.env` file in the root directory:
   ```bash
   # PostgreSQL Configuration
   POSTGRES_USER=your_username
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=your_dbname
   
   # ClickHouse Configuration
   CLICKHOUSE_USER=your_username
   CLICKHOUSE_PASSWORD=your_password
   CLICKHOUSE_DB=your_dbname
   
   # Airbyte Configuration (if using)
   AIRBYTE_CONNECTION_ID=your_connection_id
   AIRBYTE_USERNAME=your_username
   AIRBYTE_PASSWORD=your_password
   ```

3. **Start all services:**
   ```bash
   docker-compose up -d
   ```

4. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

   Expected output:
   ```
   CONTAINER ID    NAMES                    STATUS
   ...              postgres-source-db       Up (healthy)
   ...              clickhouse-analytics-db  Up (healthy)
   ...              dagster-orchestrator     Up
   ...              superset-viz             Up
   ...              pipeline_grafana         Up
   ```

5. **Generate sample data:**
   ```bash
   pip install psycopg2-binary python-dotenv
   python generate_source_data.py
   ```

### Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dagster UI** | http://localhost:3000 | - |
| **Superset** | http://localhost:8088 | admin / admin |
| **Grafana** | http://localhost:3001 | admin / admin |
| **ClickHouse HTTP** | http://localhost:8123 | default / password |
| **PostgreSQL** | localhost:5433 | retail_user / password |

## 📂 Project Structure

```
retail-analytics-pipeline/
│
├── docker-compose.yml              # Container orchestration & service definitions
├── generate_source_data.py          # Sample data generation script
├── orchestrator.py                  # Dagster job definitions & scheduling
│
├── dags_dagster/                    # Dagster DAG definitions
│   └── [Orchestration job configs]
│
├── transform_dbt/                   # DBT project for SQL transformations
│   ├── models/
│   │   ├── bronze/                 # Raw data models (minimal processing)
│   │   ├── silver/                 # Cleaned & validated data
│   │   └── gold/                   # Analytics-ready aggregations
│   ├── tests/                      # Data quality tests
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── clickhouse_config/               # ClickHouse server configurations
│   └── [Custom settings & optimizations]
│
├── airbyte/                         # Airbyte connector definitions
│   └── [CDC pipeline configurations]
│
├── superset/                        # Superset dashboards & charts
│   └── [Business intelligence assets]
│
└── README.md                        # This file
```


## 🔧 Configuration

### Dagster Job Configuration

The `orchestrator.py` file defines the pipeline:

```python
# Hourly execution schedule
retail_pipeline_schedule = ScheduleDefinition(
    name="hourly_pipeline_sync_schedule",
    job=retail_pipeline_automation_job,
    cron_schedule="0 * * * *",  # Runs at the top of every hour
)
```

**To modify the schedule:**
Edit the `cron_schedule` parameter in `orchestrator.py`:
- `"0 * * * *"` → Every hour
- `"0 0 * * *"` → Daily at midnight
- `"0 8 * * 1"` → Weekly on Monday at 8 AM


## 📋 Common Tasks

### View Pipeline Logs
```bash
# Dagster logs
docker-compose logs -f dagster

# All services
docker-compose logs -f
```

### Query Data Directly

**Via ClickHouse CLI:**
```bash
docker-compose exec clickhouse clickhouse-client -u default -p password
```

**Via PostgreSQL (source):**
```bash
docker-compose exec postgres_source psql -U retail_user -d retail_db
```

### Rebuild Transformations
```bash
docker-compose exec dagster bash -c "cd /opt/dagster/app && dbt run --models +tag:daily_refresh"
```

### Restart a Specific Service
```bash
docker-compose restart clickhouse
docker-compose restart dagster
```

### Stop the Pipeline
```bash
docker-compose down

# With volume cleanup (⚠️ removes data)
docker-compose down -v
```

## 🧪 Data Quality & Testing

DBT includes built-in tests for data integrity:

- **Uniqueness tests:** Ensure no duplicate keys
- **Not null checks:** Validate required fields
- **Referential integrity:** Foreign key validation
- **Custom SQL tests:** Business logic validation

View test results in Dagster UI after each run.

## 📈 Performance Considerations

### ClickHouse Optimizations
- **Partitioning:** Tables partitioned by date for faster queries
- **Primary Keys:** Optimized for common filter patterns
- **Compression:** Default codec reduces storage by 70-80%
- **Replication:** (Optional) Can be configured for HA

### Scaling
- Increase DBT parallelism: `dbt run --threads 8`
- Tune ClickHouse memory: Modify `docker-compose.yml` memory limits
- Consider sharding for multi-node setups

## 🚨 Troubleshooting

### Services Won't Start
```bash
# Check Docker daemon
docker --version

# Increase Docker memory
# Docker Desktop → Preferences → Resources → Memory: 8GB+

# Rebuild images
docker-compose up -d --build
```

### Data Not Appearing in ClickHouse
1. Verify PostgreSQL has data: `SELECT COUNT(*) FROM raw_transactions;`
2. Check Airbyte sync status in UI
3. Review Dagster logs for transformation errors
4. Query ClickHouse: `SELECT COUNT(*) FROM raw.raw_transactions;`

### Connection Errors
```bash
# Test PostgreSQL
docker-compose exec postgres_source psycopg2 -h localhost -U retail_user

# Test ClickHouse
docker-compose exec clickhouse clickhouse-client --host localhost
```

## 📚 Resources

- **DBT Documentation:** https://docs.getdbt.com/
- **ClickHouse Docs:** https://clickhouse.com/docs/
- **Dagster Documentation:** https://docs.dagster.io/
- **Airbyte Docs:** https://docs.airbyte.com/
- **Apache Superset:** https://superset.apache.org/

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

**Olawale Oluwafemi**
- GitHub: [@OlawaleOluwafemi](https://github.com/OlawaleOluwafemi)

## ⭐ Show Your Support

If you found this project helpful, please consider giving it a star! ⭐

---

**Last Updated:** June 21, 2026
**Status:** Active Development
