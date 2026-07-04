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