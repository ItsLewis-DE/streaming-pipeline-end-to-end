
2.6 PII Masking (GDPR readiness)
- userId, firstName, lastName, zip, lon/lat → hash hoặc mask ở Silver layer
- Lưu mapping ở bảng riêng nếu cần reversible
PHASE 3: GOLD LAYER DESIGN & IMPLEMENTATION
3.1 Gold Tables Schema
Bảng	Loại	Mô tả
gold.fact_user_activity_daily	Fact	Aggregated per user per day
gold.fact_hourly_activity	Fact	Hourly KPIs (active users, events, paid ratio)
gold.fact_top_content_daily	Fact	Top songs/artists per day ranked
gold.dim_user	Dim (SCD2)	User dimension with level history
gold.dim_song	Dim	Song/artist reference
3.2 Gold Spark Jobs (thư mục spark/app/gold/)
File	Chức năng
silver_dq_checks.py	Pre-Gold DQ validation
build_dimensions.py	Build dim_user (SCD2) + dim_song
build_fact_user_activity.py	Build fact_user_activity_daily
build_fact_hourly.py	Build fact_hourly_activity
build_top_content.py	Build fact_top_content_daily
gold_dq_checks.py	Post-Gold DQ validation
3.3 Gold DAG (airflow/dags/silver_to_gold_pipeline.py)
# schedule @daily, depends_on_past=True, catchup=True
# Flow: start → silver_dq → build_dimensions → [fact_user, fact_hourly, top_content] → gold_dq → end
PHASE 4: MONITORING & ALERTING
4.1 Grafana Dashboards
- pipeline-health.json: Kafka lag, Spark duration, data freshness per layer
- kafka-overview.json: Messages/sec, partition size, consumer groups
- spark-streaming.json: Input rate, processing rate, batch duration
- data-quality.json: DQ check pass/fail trends
- business-kpis.json: Active users, paid ratio, top songs
4.2 Prometheus Alert Rules
Tạo monitoring/prometheus/alert.rules.yml:
- DataFreshnessLag: Silver/Gold data > 2h stale
- DLQGrowing: DLQ records tăng > 10/30min
- KafkaConsumerLag: Lag > 100k messages
- SparkStreamingStopped: Streaming job not running
4.3 Alertmanager
- Thêm service alertmanager vào docker-compose
- Cấu hình route alerts tới Slack/Email
PHASE 5: TESTING
5.1 Unit Tests (tests/unit/)
- test_schemas.py: Validate schema match thực tế Eventsim output
- test_job_control.py: Test incremental logic, edge cases
- test_bronze_to_silver_transforms.py: Test dedup, timestamp conversion
- test_gold_aggregations.py: Test aggregation correctness
5.2 Integration Tests (tests/integration/)
- test_kafka_to_bronze.py: E2E Kafka → MinIO với sample data
- test_bronze_to_silver_incremental.py: Incremental processing
- test_full_pipeline.py: Bronze → Silver → Gold
5.3 Test Data (tests/data/)
- Sample JSON cho mỗi topic
- Sample corrupt records
- Expected output fixtures
PHASE 6: INFRASTRUCTURE & DEVOPS
6.1 CI/CD (.github/workflows/ci.yml)
- Lint với ruff
- Unit test với pytest
- DAG validation
- Security scan với bandit
6.2 Security
- Tạo .env.secret cho credentials, thêm vào .gitignore
- Dùng Docker secrets hoặc Vault cho production
- SSL/TLS cho inter-service communication
6.3 Backup Strategy
- Script pg_dump cho Postgres định kỳ
- MinIO bucket replication hoặc mc mirror cron job
6.4 Documentation
- README.md: Tổng quan, quick start
- docs/ARCHITECTURE.md: Kiến trúc, data flow diagram
- docs/SETUP.md: Dev environment setup
- docs/DATA_DICTIONARY.md: Schema tất cả bảng mọi layer
- docs/OPERATIONS.md: Runbook, troubleshooting
THỨ TỰ ƯU TIÊN
Phase 1 (bugs) → Phase 2 (silver enhance) → Phase 3 (gold layer) → Phase 4 (monitoring) → Phase 5 (testing) → Phase 6 (devops)
FILE ĐẦU RA
File PRODUCTION_ROADMAP.md sẽ chứa tất cả nội dung trên với:
- Checklist checkbox cho từng task
- Code snippets cụ thể
- Priority matrix (Critical/High/Medium)
- Effort estimate (giờ)
- Cấu trúc thư mục đề xuất sau hoàn thiện