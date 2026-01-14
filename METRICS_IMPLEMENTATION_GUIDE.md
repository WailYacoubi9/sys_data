# 📊 Metrics Implementation Guide

## 🎯 Overview

This guide explains how to use the new **Prometheus metrics system** integrated into the Spark Streaming cyber attack detection pipeline.

### ✅ What's Implemented

We've added **15 essential metrics** across 6 categories:

1. **Security Metrics** (5) - Attack detection tracking
2. **Performance Metrics** (3) - System throughput and latency
3. **ML Model Metrics** (3) - Model inference performance
4. **Data Quality Metrics** (2) - Input validation
5. **System Health Metrics** (1) - Spark batch processing
6. **Network Analysis Metrics** (2) - Traffic patterns

---

## 📁 New Files Created

```
spark/
├── spark_job_with_metrics.py          # Enhanced Spark job with Prometheus
├── model_utils_with_metrics.py        # ML model wrapper with metrics
└── spark-metrics-servicemonitor.yaml  # Prometheus scrape config

monitoring/
├── security-overview-dashboard.json    # Grafana dashboard: Attack detection
└── system-health-dashboard.json        # Grafana dashboard: Performance
```

---

## 🚀 Quick Start

### Step 1: Deploy Spark Job with Metrics

```bash
# Copy the new files to spark-client pod
kubectl cp spark/spark_job_with_metrics.py spark-client-0:/opt/spark/work-dir/spark_job.py
kubectl cp spark/model_utils_with_metrics.py spark-client-0:/opt/spark/work-dir/model_utils.py
kubectl cp spark/pretrained_models/ spark-client-0:/opt/spark/work-dir/pretrained_models/

# Install prometheus_client in the pod
kubectl exec spark-client-0 -- pip install prometheus_client

# Submit the Spark job
kubectl exec spark-client-0 -- /bin/bash /opt/spark/work-dir/spark_submit.sh
```

### Step 2: Configure Prometheus Scraping

```bash
# Deploy ServiceMonitor
kubectl apply -f spark/spark-metrics-servicemonitor.yaml

# Verify service is created
kubectl get svc spark-client-metrics

# Check Prometheus targets (after 30 seconds)
# Open http://localhost:9090/targets
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
```

### Step 3: Import Grafana Dashboards

```bash
# Method 1: Via Grafana UI
# 1. Open Grafana: make access-grafana (or port-forward)
# 2. Go to Dashboards → Import
# 3. Upload monitoring/security-overview-dashboard.json
# 4. Upload monitoring/system-health-dashboard.json

# Method 2: Via ConfigMap (automatic)
kubectl create configmap security-dashboard -n monitoring \
  --from-file=monitoring/security-overview-dashboard.json \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap system-health-dashboard -n monitoring \
  --from-file=monitoring/system-health-dashboard.json \
  --dry-run=client -o yaml | kubectl apply -f -
```

---

## 📊 Metrics Catalog

### 1. Security Metrics (Priority: CRITICAL)

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `spark_predictions_total` | Counter | Total ML predictions | `prediction`: malicious, benign |
| `spark_attacks_by_type_total` | Counter | Attacks by UNSW-NB15 category | `attack_type`: DoS, Exploits, etc. |
| `spark_malicious_rate_per_second` | Gauge | Current attack rate | - |
| `spark_attacks_by_protocol_total` | Counter | Malicious traffic by protocol | `protocol`, `prediction` |
| `spark_attacks_by_service_total` | Counter | Malicious traffic by service | `service`, `prediction` |

**Example Queries:**
```promql
# Malicious traffic percentage
(spark_predictions_total{prediction="malicious"} /
 (spark_predictions_total{prediction="malicious"} +
  spark_predictions_total{prediction="benign"})) * 100

# Attack rate (attacks per second)
rate(spark_predictions_total{prediction="malicious"}[1m])

# Top attacked services
topk(5, spark_attacks_by_service_total{prediction="malicious"})
```

### 2. Performance Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `spark_kafka_consumer_lag_records` | Gauge | Records behind Kafka | `topic`, `partition` |
| `spark_streaming_batch_duration_seconds` | Histogram | Batch processing time | - |
| `spark_e2e_latency_seconds` | Histogram | End-to-end latency | - |

**Critical Alerts:**
```yaml
# Alert if Spark is falling behind
- alert: KafkaConsumerLagHigh
  expr: spark_kafka_consumer_lag_records > 10000
  for: 2m
  severity: critical

# Alert if batch processing is slow
- alert: BatchProcessingSlow
  expr: histogram_quantile(0.95, spark_streaming_batch_duration_seconds_bucket) > 10
  for: 1m
  severity: warning
```

### 3. ML Model Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `spark_model_inference_duration_seconds` | Histogram | ML inference latency | - |
| `spark_model_predictions_per_second` | Gauge | Model throughput | - |
| `spark_model_errors_total` | Counter | Inference errors | `error_type` |

**Example Queries:**
```promql
# P99 inference latency
histogram_quantile(0.99, spark_model_inference_duration_seconds_bucket)

# Model throughput
spark_model_predictions_per_second

# Error rate
rate(spark_model_errors_total[5m])
```

### 4. Data Quality Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `spark_records_processed_total` | Counter | Records processed | `status`: valid, invalid |
| `spark_parsing_errors_total` | Counter | JSON parsing errors | `error_type` |

**Example Queries:**
```promql
# Invalid record rate
rate(spark_records_processed_total{status="invalid"}[5m]) /
rate(spark_records_processed_total[5m]) * 100

# Total parsing errors
spark_parsing_errors_total
```

### 5. System Health Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `spark_streaming_active_batches` | Gauge | Currently processing batches | - |
| `spark_executor_memory_used_mb` | Gauge | Executor memory usage | - |

---

## 📈 Grafana Dashboards

### Dashboard 1: Security Overview

**Purpose:** Monitor cyber attack detection in real-time

**Key Panels:**
- 🔴 Total Malicious Predictions (Stat)
- 🟢 Total Benign Predictions (Stat)
- ⚠️ Malicious Traffic Percentage (Gauge)
- 🚨 Attack Rate per Second (Stat)
- 📈 Attack Detection Rate Over Time (Time Series)
- 🌐 Malicious Traffic by Protocol (Bar Chart)
- 🎯 Top 10 Targeted Services (Bar Chart)
- 🥧 Traffic Classification Distribution (Pie Chart)
- ✅ Valid/Invalid Records (Stats)

**Access:**
```bash
# Open Grafana
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80

# URL: http://localhost:3000
# User: admin | Password: admin
# Navigate to: Dashboards → Security Overview
```

### Dashboard 2: System Health

**Purpose:** Monitor Spark Streaming performance and stability

**Key Panels:**
- 📡 Kafka Consumer Lag (Stat)
- ⏱️ Batch Duration P95 (Stat)
- 🚀 End-to-End Latency P99 (Stat)
- 📊 Batch Processing Duration (Time Series)
- 🤖 ML Model Inference Latency (Time Series)
- ⚖️ Ingestion vs Processing Rate (Time Series)
- 🔄 Active Spark Batches (Stat)
- ❌ ML Model Errors (Stat)
- ✅ Record Processing Status (Time Series)
- ⚠️ Parsing Errors Rate (Time Series)

---

## 🔍 Troubleshooting

### Problem: Metrics not showing in Prometheus

**Check 1: Verify metrics endpoint**
```bash
# Port-forward to spark-client
kubectl port-forward spark-client-0 8000:8000

# Test metrics endpoint
curl http://localhost:8000/metrics | grep spark_

# Should see output like:
# spark_predictions_total{prediction="malicious"} 42.0
# spark_predictions_total{prediction="benign"} 158.0
```

**Check 2: Verify ServiceMonitor**
```bash
# Check if ServiceMonitor exists
kubectl get servicemonitor -n monitoring spark-client-metrics

# Check Prometheus config
kubectl get prometheus -n monitoring -o yaml | grep spark
```

**Check 3: Check Prometheus targets**
```bash
# Open Prometheus UI
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090

# Go to: http://localhost:9090/targets
# Look for: default/spark-client-metrics/metrics
# Status should be: UP (green)
```

### Problem: Spark job crashes with ImportError

```bash
# Install prometheus_client in pod
kubectl exec spark-client-0 -- pip install prometheus_client

# Verify installation
kubectl exec spark-client-0 -- python3 -c "import prometheus_client; print('OK')"
```

### Problem: Dashboards show "No Data"

**Solution 1: Wait for data**
- Metrics are scraped every 30 seconds
- Wait 1-2 minutes after starting the Spark job

**Solution 2: Check data source**
- In Grafana, go to Configuration → Data Sources
- Verify "Prometheus" exists with URL: `http://kube-prometheus-stack-prometheus:9090`

**Solution 3: Test queries manually**
```bash
# In Grafana, go to Explore
# Run test query:
spark_predictions_total

# Should return results
```

---

## 🎯 Key Monitoring Scenarios

### Scenario 1: Detecting Attack Spikes

**Query:**
```promql
# Attack rate compared to baseline
rate(spark_predictions_total{prediction="malicious"}[1m]) >
avg_over_time(rate(spark_predictions_total{prediction="malicious"}[1m])[1h:1m]) * 3
```

**Alert:**
```yaml
- alert: AttackSpike
  expr: rate(spark_predictions_total{prediction="malicious"}[1m]) > 10
  for: 1m
  annotations:
    summary: "High attack rate detected"
```

### Scenario 2: Performance Degradation

**Check:**
1. Kafka Lag increasing? → Spark can't keep up
2. Batch Duration > 10s? → Processing too slow
3. ML Inference P99 > 1s? → Model bottleneck

**Query:**
```promql
# Backpressure detection
spark_streaming_batch_duration_seconds > 5
```

### Scenario 3: Data Quality Issues

**Check:**
```promql
# High invalid record rate (>5%)
(rate(spark_records_processed_total{status="invalid"}[5m]) /
 rate(spark_records_processed_total[5m])) > 0.05
```

---

## 📝 Best Practices

### 1. Metrics Naming Convention

✅ **Good:**
- `spark_predictions_total` (noun, plural)
- `spark_model_inference_duration_seconds` (base unit: seconds)
- `spark_records_processed_total` (clear meaning)

❌ **Bad:**
- `predictions` (too vague)
- `inference_time_ms` (non-standard unit)
- `processed` (unclear what is being counted)

### 2. Label Usage

✅ **Use labels for dimensions:**
```python
predictions_counter.labels(prediction='malicious').inc()
attacks_by_protocol.labels(protocol='tcp', prediction='malicious').inc()
```

❌ **Don't create separate metrics:**
```python
# BAD: Don't do this
malicious_counter = Counter('malicious_predictions')
benign_counter = Counter('benign_predictions')
```

### 3. Histogram Buckets

Choose buckets based on expected latency:
```python
# For sub-second operations (ML inference)
buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0]

# For batch operations (seconds to minutes)
buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
```

---

## 🔮 Future Enhancements

### Phase 2 (Nice to Have):

1. **Attack Type Tracking**
   - Requires keeping `attack_cat` in Kafka messages
   - Track 9 UNSW-NB15 attack categories

2. **Anomaly Detection**
   - Baseline attack rate calculation
   - Spike detection algorithm
   - Zero-day candidate identification

3. **Advanced Network Analysis**
   - TCP connection state tracking
   - Packet loss correlation
   - Flow duration distribution

4. **Resource Monitoring**
   - JVM heap usage tracking
   - GC pause time metrics
   - CPU/Memory per executor

---

## 📚 Additional Resources

- **Prometheus Docs:** https://prometheus.io/docs/
- **Grafana Dashboards:** https://grafana.com/docs/grafana/latest/dashboards/
- **Spark Monitoring:** https://spark.apache.org/docs/latest/monitoring.html
- **UNSW-NB15 Dataset:** https://research.unsw.edu.au/projects/unsw-nb15-dataset

---

## ✅ Verification Checklist

Before considering the implementation complete:

- [ ] Spark job starts without errors
- [ ] Metrics endpoint responds at `:8000/metrics`
- [ ] Prometheus scrapes metrics successfully (check targets)
- [ ] Grafana dashboards show data (wait 2 min)
- [ ] Security dashboard shows malicious/benign counts
- [ ] System health dashboard shows batch duration
- [ ] Can query metrics in Prometheus UI
- [ ] Can create custom dashboards in Grafana

---

**Created by:** Claude
**Date:** 2026-01-14
**Version:** 1.0
