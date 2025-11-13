# Metrics Guide for Kafka + Spark Pipeline

## Current Available Metrics (Kubernetes Only)

Your current `kube-prometheus-stack` setup provides **Kubernetes infrastructure metrics** only. These are collected from:
- **cAdvisor** - Container resource usage
- **kube-state-metrics** - Kubernetes object states
- **Node Exporter** - System-level metrics

### ✅ Working PromQL Queries

#### Pod Resource Usage
```promql
# CPU usage by pod (cores)
sum(rate(container_cpu_usage_seconds_total{namespace="default"}[5m])) by (pod)

# Memory usage by pod (MB)
sum(container_memory_working_set_bytes{namespace="default"}) by (pod) / 1024 / 1024

# Memory usage percentage
(container_memory_working_set_bytes{namespace="default"} / container_spec_memory_limit_bytes{namespace="default"}) * 100

# Disk writes per pod (MB/s)
sum(rate(container_fs_writes_bytes_total{namespace="default"}[5m])) by (pod) / 1024 / 1024

# Disk reads per pod (MB/s)
sum(rate(container_fs_reads_bytes_total{namespace="default"}[5m])) by (pod) / 1024 / 1024
```

#### Pod Health
```promql
# Pod restart count
kube_pod_container_status_restarts_total{namespace="default"}

# Recent restarts (last 5 min)
increase(kube_pod_container_status_restarts_total{namespace="default"}[5m])

# Running pods count
count(kube_pod_status_phase{namespace="default", phase="Running"})

# Pod ready status
kube_pod_status_ready{namespace="default", condition="true"}
```

#### Network (May Not Be Available)
```promql
# Network receive bytes/sec
sum(rate(container_network_receive_bytes_total{namespace="default"}[5m])) by (pod)

# Network transmit bytes/sec
sum(rate(container_network_transmit_bytes_total{namespace="default"}[5m])) by (pod)
```

**Note:** Network metrics may not be available in all Kubernetes setups. If you get "No data", this metric is not exposed.

---

## Missing Metrics (Require Configuration)

### ❌ Kafka Application Metrics (Not Available Yet)

These metrics require **JMX Exporter**:
- `kafka_server_brokertopicmetrics_messagesin_total` - Messages per second
- `kafka_server_brokertopicmetrics_bytesin_total` - Bytes in
- `kafka_server_brokertopicmetrics_bytesout_total` - Bytes out
- `kafka_consumergroup_lag` - Consumer lag
- `kafka_server_replicamanager_underreplicatedpartitions` - Health check

### ❌ Spark Application Metrics (Not Available Yet)

These metrics require **Spark Prometheus Sink**:
- `metrics_spark_app_driver_streaming_inputRowsPerSecond` - Input rate
- `metrics_spark_app_driver_streaming_processedRowsPerSecond` - Processing rate
- `metrics_spark_app_driver_DAGScheduler_job_activeJobs_Number` - Active jobs
- `metrics_spark_app_executor_memoryUsed_Value` - Executor memory

---

## How to Add Application Metrics

### Step 1: Add JMX Exporter to Kafka

To get Kafka-specific metrics, you need to:

1. **Download JMX Exporter JAR** into Kafka pods
2. **Configure KAFKA_OPTS** to enable JMX
3. **Add ServiceMonitor** for Prometheus scraping

**Example modification to `kafka_broker_statefulset.yaml`:**

```yaml
containers:
  - name: broker
    image: apache/kafka:3.8.0
    env:
      - name: KAFKA_OPTS
        value: "-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx_exporter/kafka-broker.yml"
    ports:
      - containerPort: 7071  # JMX metrics port
        name: jmx-metrics
```

### Step 2: Add Prometheus Sink to Spark

To get Spark metrics, configure Spark to push metrics to Prometheus:

**Add to Spark configuration:**

```properties
# spark-defaults.conf
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheus.path=/metrics
spark.ui.prometheus.enabled=true
```

**Add ServiceMonitor:**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: spark-metrics
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: spark-master
  endpoints:
    - port: webui
      path: /metrics/prometheus
```

---

## Recommended Grafana Dashboard (Current Metrics)

Use these queries in your Grafana dashboard **right now**:

### Panel 1: Pod CPU Usage
```promql
sum(rate(container_cpu_usage_seconds_total{namespace="default"}[5m])) by (pod)
```
**Visualization:** Time series

### Panel 2: Pod Memory Usage
```promql
sum(container_memory_working_set_bytes{namespace="default"}) by (pod) / 1024 / 1024
```
**Visualization:** Time series
**Unit:** MB

### Panel 3: Kafka Broker Disk Writes
```promql
sum(rate(container_fs_writes_bytes_total{pod=~"kafka-broker.*"}[5m])) by (pod) / 1024 / 1024
```
**Visualization:** Time series
**Unit:** MB/s

### Panel 4: Pod Restart Count
```promql
kube_pod_container_status_restarts_total{namespace="default"}
```
**Visualization:** Stat
**Thresholds:** Green (0), Yellow (>3), Red (>5)

### Panel 5: Spark Worker CPU
```promql
sum(rate(container_cpu_usage_seconds_total{pod=~"spark-worker.*"}[5m])) by (pod)
```
**Visualization:** Time series

### Panel 6: Total Pipeline Memory
```promql
sum(container_memory_working_set_bytes{namespace="default"}) / 1024 / 1024 / 1024
```
**Visualization:** Gauge
**Unit:** GB

---

## Testing Your Queries

### In Prometheus (http://localhost:9090)

1. Go to **Graph** tab
2. Paste query
3. Click **Execute**
4. Check the **Table** or **Graph** view

### In Grafana (http://localhost:3000)

1. Go to **Explore** (left sidebar)
2. Select **Prometheus** data source
3. Paste query in **Metrics browser**
4. Click **Run query**

---

## Troubleshooting

### "No data" in Grafana

**Check 1:** Query works in Prometheus?
- Open http://localhost:9090
- Try the query there first

**Check 2:** Time range includes data?
- Change time range to "Last 1 hour" or "Last 6 hours"

**Check 3:** Pods are running?
```bash
kubectl get pods
```

**Check 4:** Metrics exist?
```promql
# Try simplest query first
up{namespace="default"}
```

### Network Metrics Not Available

If `container_network_receive_bytes_total` returns no data:
- Network metrics are not exposed by your Kubernetes setup
- This is common in Minikube with certain drivers
- Focus on CPU, memory, and disk metrics instead

### Want Application-Level Metrics?

You'll need to:
1. Add JMX Exporter to Kafka (advanced)
2. Add Prometheus sink to Spark (advanced)
3. Create custom ServiceMonitors

Let me know if you want help implementing these!
