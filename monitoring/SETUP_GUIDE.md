# Complete Monitoring Setup Guide

This guide explains how to set up comprehensive monitoring for your Kafka + Spark data processing pipeline with application-level metrics.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│           Kubernetes Cluster (Minikube)              │
│                                                       │
│  ┌──────────────┐        ┌──────────────┐          │
│  │ Kafka Broker │◄───────┤ JMX Exporter │          │
│  │   :19092     │  JMX   │    :7071     │          │
│  │   :9999      │        │  /metrics    │          │
│  └──────────────┘        └──────┬───────┘          │
│                                  │                   │
│  ┌──────────────┐        ┌──────┴───────┐          │
│  │ Spark Master │───────►│ Prometheus   │          │
│  │   :8080      │ scrape │ Servlet      │          │
│  │   :4040      │        │ /metrics/    │          │
│  │              │        │ prometheus   │          │
│  └──────────────┘        └──────┬───────┘          │
│                                  │                   │
│                          ┌───────▼───────┐          │
│                          │  Prometheus   │          │
│                          │   (Storage)   │          │
│                          └───────┬───────┘          │
│                                  │                   │
│                          ┌───────▼───────┐          │
│                          │    Grafana    │          │
│                          │ (Dashboards)  │          │
│                          └───────────────┘          │
└─────────────────────────────────────────────────────┘
```

## What's New

Inspired by [k8s-data-processing](https://github.com/medyassineakhmari/k8s-data-processing), we've added:

### 1. **ServiceMonitors** (`servicemonitors.yaml`)
- Kafka broker metrics monitoring
- Kafka controller metrics monitoring
- Spark master metrics monitoring
- Spark worker metrics monitoring

### 2. **JMX Exporter for Kafka** (`kafka-jmx-config.yaml`, `kafka-broker-with-metrics.yaml`)
- Sidecar container running JMX-to-Prometheus exporter
- Exposes Kafka broker metrics at `:7071/metrics`
- Monitors: throughput, request rates, replication, JVM metrics

### 3. **Spark Prometheus Metrics** (`spark-metrics-config.yaml`, `spark-*-with-metrics.yaml`)
- Built-in Spark PrometheusServlet integration
- Exposes metrics at `:4040/metrics/prometheus` (master) and `:4041/metrics/prometheus` (worker)
- Monitors: JVM metrics, executor stats, job metrics

### 4. **Enhanced Makefile Targets**
- `setup-full-monitoring` - One-command setup for everything
- `setup-metrics-exporters` - Deploy ConfigMaps and ServiceMonitors
- `deploy-kafka-with-metrics` - Deploy Kafka with JMX exporter
- `deploy-spark-with-metrics` - Deploy Spark with Prometheus metrics

## Installation

### Quick Start (Recommended)

```bash
# 1. Start Minikube
make start-minikube

# 2. Deploy Kafka controller (unchanged)
kubectl apply -f apache_kafka/kafka_controller_statefulset.yaml

# 3. Setup full monitoring stack (Prometheus + Grafana + Metrics)
make setup-full-monitoring

# 4. Deploy Python producer
make start_python_producer

# 5. Launch producer
make launch_producer

# 6. Submit Spark job
make submit_spark_job
```

### Step-by-Step Installation

```bash
# 1. Install Prometheus + Grafana
make setup-monitoring

# 2. Setup metrics exporters and ServiceMonitors
make setup-metrics-exporters

# 3. Deploy Kafka with JMX metrics
make deploy-kafka-with-metrics

# 4. Deploy Spark with Prometheus metrics
make deploy-spark-with-metrics
```

## Verification

### 1. Check Pod Status

```bash
kubectl get pods

# You should see:
# kafka-broker-0           2/2     Running   # Note: 2/2 (main + jmx-exporter)
# kafka-broker-1           2/2     Running
# spark-master-xxx         1/1     Running
# spark-worker-xxx         1/1     Running
```

### 2. Check Prometheus Targets

```bash
make access-prometheus
# Open http://localhost:9090/targets

# You should see these ServiceMonitors as UP:
# ✅ serviceMonitor/default/kafka-broker-metrics/0
# ✅ serviceMonitor/default/spark-master-metrics/0
# ✅ serviceMonitor/default/spark-worker-metrics/0
```

### 3. Test Metrics Endpoints

```bash
# Test Kafka JMX metrics
kubectl exec -it kafka-broker-0 -c jmx-exporter -- curl localhost:7071/metrics

# Test Spark master metrics
kubectl exec -it <spark-master-pod> -- curl localhost:4040/metrics/prometheus

# Test Spark worker metrics
kubectl exec -it <spark-worker-pod> -- curl localhost:4041/metrics/prometheus
```

## Available Metrics

### Kafka Metrics (via JMX Exporter)

```promql
# Messages in per second
rate(kafka_server_brokertopicmetrics_messagesinpersec_count[5m])

# Bytes in per second
rate(kafka_server_brokertopicmetrics_bytesinpersec_count[5m])

# Bytes out per second
rate(kafka_server_brokertopicmetrics_bytesoutpersec_count[5m])

# Under-replicated partitions (should be 0)
kafka_server_replicamanager_underreplicatedpartitions_value

# Active controller count (should be 1)
kafka_controller_kafkacontroller_activecontrollercount_value

# Request handler idle percentage
kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_count
```

### Spark Metrics (via PrometheusServlet)

```promql
# JVM heap memory usage
metrics_jvm_heap_used

# JVM garbage collection time
rate(metrics_jvm_PS_MarkSweep_time[5m])

# Executor metrics (when jobs are running)
metrics_executor_*

# Driver metrics
metrics_driver_*
```

### Kubernetes Infrastructure Metrics (Always Available)

```promql
# Pod CPU usage
sum(rate(container_cpu_usage_seconds_total{namespace="default"}[5m])) by (pod)

# Pod memory usage (MB)
sum(container_memory_working_set_bytes{namespace="default"}) by (pod) / 1024 / 1024

# Pod restarts
kube_pod_container_status_restarts_total{namespace="default"}

# Disk I/O
rate(container_fs_writes_bytes_total{namespace="default"}[5m])
```

## Grafana Dashboard Setup

### 1. Access Grafana

```bash
make access-grafana
# Open http://localhost:3000
# Username: admin
# Password: admin
```

### 2. Create Dashboard

Go to **Dashboards** → **New Dashboard** → **Add visualization**

### 3. Add Panels

#### Panel 1: Kafka Throughput
```promql
# Query A: Messages In
rate(kafka_server_brokertopicmetrics_messagesinpersec_count[5m])

# Query B: Bytes In
rate(kafka_server_brokertopicmetrics_bytesinpersec_count[5m])
```
**Visualization:** Time series
**Unit:** Messages/sec, Bytes/sec

#### Panel 2: Spark JVM Memory
```promql
metrics_jvm_heap_used
```
**Visualization:** Time series
**Unit:** Bytes

#### Panel 3: Pod CPU Usage
```promql
sum(rate(container_cpu_usage_seconds_total{namespace="default"}[5m])) by (pod)
```
**Visualization:** Time series
**Unit:** Cores

#### Panel 4: Kafka Under-Replicated Partitions
```promql
kafka_server_replicamanager_underreplicatedpartitions_value
```
**Visualization:** Stat
**Threshold:** Green (0), Red (>0)

### 4. Import Pre-built Dashboard (Optional)

You can import the dashboard JSON from `monitoring/grafana-dashboard-kafka-spark.json`:
1. Go to **Dashboards** → **New** → **Import**
2. Upload the JSON file
3. Select Prometheus as data source

## Troubleshooting

### JMX Exporter Not Working

```bash
# Check if JMX exporter container is running
kubectl describe pod kafka-broker-0

# Check JMX exporter logs
kubectl logs kafka-broker-0 -c jmx-exporter

# Check if JMX port is accessible
kubectl exec -it kafka-broker-0 -c broker -- netstat -tuln | grep 9999
```

### Spark Metrics Not Showing

```bash
# Check if metrics.properties is loaded
kubectl exec -it <spark-master-pod> -- cat /opt/spark/conf/metrics.properties

# Check Spark logs
kubectl logs <spark-master-pod>

# Test metrics endpoint
kubectl exec -it <spark-master-pod> -- curl localhost:4040/metrics/prometheus
```

### ServiceMonitor Not Discovered

```bash
# Check ServiceMonitor status
kubectl get servicemonitor -A

# Check Prometheus configuration
kubectl get prometheus -n monitoring -o yaml

# Verify the ServiceMonitor selector flags are set
helm get values kube-prometheus-stack -n monitoring
# Should show:
#   serviceMonitorSelectorNilUsesHelmValues: false
#   podMonitorSelectorNilUsesHelmValues: false
```

### Metrics Queries Return No Data

1. **Check time range** - Ensure Grafana time range includes recent data
2. **Verify targets in Prometheus** - http://localhost:9090/targets (all should be UP)
3. **Test query in Prometheus** - Try the query directly in Prometheus first
4. **Check pod labels** - ServiceMonitors match pods by labels

## Comparison: Before vs After

| Metric Type | Before | After |
|------------|--------|-------|
| **Kubernetes Metrics** | ✅ CPU, Memory, Disk | ✅ CPU, Memory, Disk |
| **Kafka Metrics** | ❌ Not available | ✅ Throughput, Lag, Health |
| **Spark Metrics** | ❌ Not available | ✅ JVM, Executors, Jobs |
| **ServiceMonitors** | ❌ None | ✅ Kafka + Spark |
| **Metrics Collection** | Manual scraping | Automated by Prometheus |

## Next Steps

### 1. Create Alerts

Add alerting rules for critical conditions:

```yaml
# Example: Alert on high consumer lag
alert: KafkaHighConsumerLag
expr: kafka_consumergroup_lag > 1000
for: 5m
annotations:
  summary: "High consumer lag detected"
```

### 2. Add Consumer Lag Monitoring

Deploy a Kafka lag exporter for detailed consumer group monitoring.

### 3. Custom Application Metrics

Instrument your Python producer and Spark job with custom metrics using Prometheus client libraries.

### 4. Set Up Grafana Alerts

Configure Grafana to send alerts via Slack, Email, or PagerDuty.

## References

- Inspired by: [k8s-data-processing](https://github.com/medyassineakhmari/k8s-data-processing)
- JMX Exporter: [Bitnami JMX Exporter](https://github.com/bitnami/containers/tree/main/bitnami/jmx-exporter)
- Spark Metrics: [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)
- Prometheus Operator: [ServiceMonitor Spec](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/api.md#servicemonitor)
