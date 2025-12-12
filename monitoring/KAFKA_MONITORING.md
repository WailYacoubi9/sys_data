# Kafka Monitoring Setup Guide

This guide explains how to set up comprehensive monitoring for Kafka using JMX Exporter, Prometheus, and Grafana.

## 📊 Overview

The monitoring stack exposes Kafka broker metrics through JMX (Java Management Extensions) and makes them available to Prometheus for scraping. Grafana is then used to visualize these metrics.

### Key Metrics Exposed

- **kafka_log_logendoffset**: Latest offset in each partition
- **kafka_log_logstartoffset**: Earliest offset in each partition
- **kafka_log_log_size**: Size of logs in bytes per partition
- **kafka_server_messages_in_total**: Total messages received by broker
- **kafka_server_topic_messages_in_total**: Messages per topic
- **jvm_memory_heap_***: JVM heap memory metrics
- **jvm_gc_collection_***: Garbage collection metrics

---

## 🚀 Quick Start

### 1. Deploy Kafka Monitoring Stack

```bash
# Deploy the complete monitoring setup
make setup-kafka-monitoring
```

This command will:
1. Create the JMX Exporter ConfigMap
2. Update Kafka broker StatefulSet with JMX configuration
3. Restart Kafka brokers with monitoring enabled
4. Create ServiceMonitor for Prometheus scraping
5. Wait for all pods to be ready

### 2. Verify Metrics are Being Scraped

```bash
# Check that everything is working
make verify-kafka-metrics
```

### 3. Import Grafana Dashboard

```bash
# Import the pre-built dashboard
make import-grafana-dashboard

# Then access Grafana
make access-grafana
# Open http://localhost:3000
# Username: admin | Password: admin
```

---

## 🔧 Manual Installation (Step by Step)

If you prefer to install components manually:

### Step 1: Create JMX Exporter ConfigMap

```bash
kubectl apply -f apache_kafka/kafka-jmx-exporter-config.yaml
```

This ConfigMap contains the JMX Exporter configuration that defines which JMX beans to export as Prometheus metrics.

### Step 2: Update Kafka Brokers with JMX Support

```bash
kubectl apply -f apache_kafka/kafka_broker_statefulset.yaml
```

This will:
- Add an initContainer to download JMX Prometheus Exporter JAR
- Configure environment variables for JMX (port 9999)
- Expose metrics endpoint on port 5556
- Mount the JMX config as a volume

### Step 3: Restart Kafka Brokers

```bash
kubectl rollout restart statefulset/kafka-broker
kubectl rollout status statefulset/kafka-broker
```

Wait for all pods to be ready (may take 1-2 minutes).

### Step 4: Create ServiceMonitor for Prometheus

```bash
kubectl apply -f apache_kafka/kafka-servicemonitor.yaml
```

This tells Prometheus to scrape metrics from Kafka brokers every 30 seconds.

### Step 5: Verify Metrics Endpoint

Test that metrics are being exposed:

```bash
# Test from broker pod
kubectl exec kafka-broker-0 -- wget -qO- localhost:5556/metrics | head -20

# You should see output like:
# kafka_log_log_size{partition="0",topic="demo"} 12345.0
# kafka_log_logendoffset{partition="0",topic="demo"} 500.0
```

---

## ✅ Verification Steps

### 1. Check Kafka Pods are Running

```bash
kubectl get pods -l app=kafka-broker

# Expected output:
# NAME             READY   STATUS    RESTARTS   AGE
# kafka-broker-0   1/1     Running   0          5m
# kafka-broker-1   1/1     Running   0          5m
```

### 2. Check Metrics Endpoint is Accessible

```bash
# Get the pod IP and test metrics endpoint
kubectl get pods -l app=kafka-broker -o wide

# Test metrics from broker-0
kubectl exec kafka-broker-0 -- wget -qO- localhost:5556/metrics | grep kafka_log
```

Expected output should contain lines like:
```
kafka_log_log_size{partition="0",topic="demo"} 1234.0
kafka_log_logendoffset{partition="0",topic="demo"} 100.0
kafka_log_logstartoffset{partition="0",topic="demo"} 0.0
```

### 3. Verify Prometheus is Scraping

```bash
# Port-forward to Prometheus
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
```

Then open http://localhost:9090 in your browser:

1. Go to **Status → Targets**
2. Search for "kafka" in the filter
3. You should see targets like `monitoring/kafka-broker-metrics/0` with state **UP**

![Prometheus Targets](https://i.imgur.com/example.png)

### 4. Test PromQL Queries

In Prometheus UI (http://localhost:9090), try these queries:

```promql
# Total messages in topic "demo"
sum(kafka_log_logendoffset{topic="demo"} - kafka_log_logstartoffset{topic="demo"})

# Messages per partition
kafka_log_logendoffset{topic="demo"} - kafka_log_logstartoffset{topic="demo"}

# Message ingestion rate (messages/sec)
rate(kafka_server_topic_messages_in_total{topic="demo"}[1m])

# Broker JVM heap memory
jvm_memory_heap_used{job="kafka-broker-metrics"}
```

If these queries return data, ✅ **monitoring is working!**

---

## 📈 Accessing Grafana Dashboard

### Option 1: Port Forwarding (Recommended)

```bash
make access-grafana
# Opens port-forward on http://localhost:3000
```

Default credentials:
- **Username**: `admin`
- **Password**: `admin`

### Option 2: NodePort

```bash
# Get the NodePort URL
echo "http://$(minikube ip):$(kubectl get svc kube-prometheus-stack-grafana -n monitoring -o jsonpath='{.spec.ports[0].nodePort}')"
```

### Importing the Dashboard

1. Log in to Grafana
2. Click **Dashboards → Import** (+ icon on left sidebar)
3. Click **Upload JSON file**
4. Select `monitoring/kafka-messages-dashboard.json`
5. Click **Import**

You should now see the **Kafka Messages Monitoring** dashboard with 7 panels:

1. **Total Messages in Topic 'demo'** - Big stat showing total message count
2. **Messages per Partition** - Time series graph showing messages per partition
3. **Message Ingestion Rate** - Messages/sec over time
4. **Log Size per Partition** - Disk space used by each partition
5. **Partition Details Table** - Summary table of all partitions
6. **Kafka Broker JVM Heap Memory** - Memory usage per broker
7. **Kafka Broker GC Rate** - Garbage collection frequency

---

## 🐛 Troubleshooting

### Issue 1: Prometheus Not Scraping Metrics

**Symptoms**: No data in Grafana, or Prometheus targets show as DOWN

**Solution**:

```bash
# 1. Check ServiceMonitor exists and has correct labels
kubectl get servicemonitor -n monitoring kafka-broker-metrics -o yaml

# Verify it has label: release: kube-prometheus-stack
# This is CRITICAL for Prometheus to discover it

# 2. Check Prometheus logs
kubectl logs -n monitoring -l app.kubernetes.io/name=prometheus -c prometheus | grep kafka

# 3. Verify the service has the metrics port
kubectl get svc kafka-broker-service -o yaml | grep -A5 ports
```

### Issue 2: JMX Exporter Not Starting

**Symptoms**: Kafka pods crash or restart repeatedly, logs show JMX errors

**Solution**:

```bash
# Check broker logs
kubectl logs kafka-broker-0 | grep -i jmx

# Common issues:
# - JAR not downloaded: Check download-jmx-exporter initContainer logs
# - Config not mounted: Check volumeMounts and configMap

# Verify JMX exporter JAR exists
kubectl exec kafka-broker-0 -- ls -la /opt/jmx-exporter/

# Should show:
# jmx_prometheus_javaagent.jar
# config.yml
```

### Issue 3: Metrics Endpoint Returns 404

**Symptoms**: `wget localhost:5556/metrics` returns 404 or connection refused

**Solution**:

```bash
# 1. Check if port 5556 is listening
kubectl exec kafka-broker-0 -- netstat -tlnp | grep 5556

# 2. Check KAFKA_OPTS environment variable is set
kubectl exec kafka-broker-0 -- env | grep KAFKA_OPTS

# Should show:
# KAFKA_OPTS=-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=5556:/opt/jmx-exporter/config.yml

# 3. Restart the broker if needed
kubectl delete pod kafka-broker-0
```

### Issue 4: No Data for Topic "demo"

**Symptoms**: Dashboard shows no messages for topic "demo"

**Solution**:

```bash
# 1. Verify topic exists and has messages
kubectl exec kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --list

# 2. Check topic offsets
kubectl exec kafka-broker-0 -- /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:19092 --topic demo --time -1

# 3. If topic doesn't exist, produce some test messages
# (Make sure your producer is running)
make launch_producer_alt
```

### Issue 5: Dashboard Shows "No Data"

**Symptoms**: Grafana dashboard loads but panels show "No data"

**Solution**:

```bash
# 1. Check data source in Grafana
# Go to Configuration → Data Sources → Prometheus
# Click "Save & Test" - should show "Data source is working"

# 2. Verify Prometheus has data
# Go to Prometheus UI → Graph tab
# Query: kafka_log_logendoffset
# Should return results

# 3. Check time range in Grafana
# Top-right corner - try "Last 15 minutes" or "Last 1 hour"

# 4. Force refresh
# Click the refresh button or set auto-refresh to 10s
```

---

## 🔍 Understanding the Metrics

### Message Count Calculation

The number of messages in a partition is calculated as:

```
Messages = LogEndOffset - LogStartOffset
```

- **LogEndOffset**: Offset of the next message to be written (latest offset + 1)
- **LogStartOffset**: Offset of the oldest message in the partition

Example:
- LogEndOffset = 1000
- LogStartOffset = 0
- Messages = 1000 - 0 = **1000 messages**

### Partition Distribution

Topic "demo" has **3 partitions** (configured in `kafka_broker_statefulset.yaml`):
- Partition 0
- Partition 1
- Partition 2

With 2 brokers, partitions are distributed:
- Broker 0 (kafka-broker-0): Hosts some partitions
- Broker 1 (kafka-broker-1): Hosts other partitions

### JMX Metrics Hierarchy

Kafka exposes hundreds of JMX beans. We filter them in `kafka-jmx-exporter-config.yaml`:

```yaml
kafka.log<type=Log, name=Size, topic=demo, partition=0>
  → kafka_log_log_size{topic="demo", partition="0"}

kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec>
  → kafka_server_messages_in_total
```

---

## 📊 Dashboard Panels Explained

### Panel 1: Total Messages in Topic 'demo'

**Query**: `sum(kafka_log_logendoffset{topic="demo"} - kafka_log_logstartoffset{topic="demo"})`

Shows the total number of messages across all partitions of topic "demo".

### Panel 2: Messages per Partition

**Query**: `kafka_log_logendoffset{topic="demo"} - kafka_log_logstartoffset{topic="demo"}`

Time series graph showing how messages are distributed across partitions. Useful for detecting:
- Unbalanced partitions
- Producer key distribution issues
- Partition growth over time

### Panel 3: Message Ingestion Rate

**Query**: `rate(kafka_server_topic_messages_in_total{topic="demo"}[1m])`

Shows how many messages/sec are being written to the topic. Useful for:
- Monitoring producer throughput
- Detecting traffic spikes
- Capacity planning

### Panel 4: Log Size per Partition

**Query**: `kafka_log_log_size{topic="demo"}`

Shows disk space used by each partition in bytes. Important for:
- Disk capacity monitoring
- Identifying partitions needing cleanup
- Retention policy verification

### Panel 5: Partition Details Table

Same query as Panel 2 but displayed as a table for quick reference.

### Panel 6: JVM Heap Memory

**Query**: `sum by (broker_id) (jvm_memory_heap_used{job="kafka-broker-metrics"})`

Shows JVM heap memory usage per broker. Watch for:
- Memory leaks (steadily increasing)
- OutOfMemoryError (heap maxing out)
- Need to increase broker memory limits

### Panel 7: GC Rate

**Query**: `sum by (broker_id) (rate(jvm_gc_collection_count{job="kafka-broker-metrics"}[1m]))`

Shows garbage collection frequency. High GC rate can indicate:
- Memory pressure
- Need for GC tuning
- Performance degradation

---

## 🎯 Next Steps

### 1. Add Alerting Rules

Create alerting rules in Prometheus for critical conditions:

```yaml
# Example: Alert when consumer lag is high
- alert: KafkaHighConsumerLag
  expr: kafka_consumergroup_lag > 10000
  for: 5m
  annotations:
    summary: "High consumer lag on topic {{ $labels.topic }}"
```

### 2. Monitor Consumer Lag

Add a consumer group exporter to track Spark consumer lag:

```bash
# Deploy kafka-exporter for consumer group metrics
kubectl apply -f monitoring/kafka-exporter.yaml
```

### 3. Add More Dashboards

Create dashboards for:
- Spark streaming metrics
- Producer performance
- Network throughput
- Disk I/O

### 4. Set Up Long-Term Storage

Configure Prometheus remote write to send metrics to long-term storage (Thanos, Cortex, etc.)

---

## 📝 Important Notes

1. **JMX Port 9999**: Used for direct JMX access (not exposed outside pod)
2. **Metrics Port 5556**: HTTP endpoint for Prometheus scraping
3. **ServiceMonitor Label**: `release: kube-prometheus-stack` is **mandatory**
4. **Resource Limits**: JMX exporter adds ~50MB memory overhead per broker
5. **Scrape Interval**: Set to 30s (configurable in ServiceMonitor)

---

## 🔗 References

- [Kafka JMX Metrics](https://kafka.apache.org/documentation/#monitoring)
- [JMX Exporter GitHub](https://github.com/prometheus/jmx_exporter)
- [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack)
- [Grafana Dashboards](https://grafana.com/grafana/dashboards/)

---

## 📞 Support

If you encounter issues:

1. Check the **Troubleshooting** section above
2. Verify all verification steps pass
3. Check Prometheus and Grafana logs
4. Review Kafka broker logs for JMX errors

---

**Dashboard Last Updated**: 2025-12-12
**Tested on**: Minikube v1.37.0, Kafka 3.8.0, Prometheus Operator v0.70.0
