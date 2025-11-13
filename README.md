# Real-Time Cybersecurity Attack Detection on Kubernetes

A distributed real-time machine learning pipeline for detecting cybersecurity attacks using Kafka, Spark Streaming, and XGBoost, deployed on Kubernetes with comprehensive Prometheus + Grafana monitoring.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster (Minikube)                 │
│                                                                   │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐  │
│  │   Python     │      │    Kafka     │      │    Spark     │  │
│  │   Producer   │─────►│   Cluster    │─────►│  Streaming   │  │
│  │              │ pub  │  (KRaft)     │ sub  │   + ML       │  │
│  │ UNSW-NB15    │      │              │      │  (XGBoost)   │  │
│  │  Dataset     │      │ 2 Brokers +  │      │              │  │
│  └──────────────┘      │ 1 Controller │      │ Attack       │  │
│                        └──────────────┘      │ Detection    │  │
│                               │               └──────────────┘  │
│                               │                      │           │
│                        ┌──────▼──────────────────────▼────┐    │
│                        │      JMX Exporter Sidecars       │    │
│                        │   + Spark Prometheus Servlet     │    │
│                        └──────┬───────────────────────────┘    │
│                               │                                 │
│                        ┌──────▼──────┐                         │
│                        │  Prometheus  │                         │
│                        │ (Monitoring) │                         │
│                        └──────┬───────┘                         │
│                               │                                 │
│                        ┌──────▼──────┐                         │
│                        │   Grafana   │                         │
│                        │ (Dashboards)│                         │
│                        └─────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Data Pipeline

1. **Data Ingestion**: Python producer reads UNSW-NB15 network traffic dataset
2. **Message Queue**: Kafka stores streaming events in the "demo" topic
3. **Stream Processing**: Spark Streaming consumes events in micro-batches
4. **ML Inference**: XGBoost binary classifier detects attacks (0=normal, 1=attack)
5. **Output**: Results printed to console (can be extended to database/API)
6. **Monitoring**: Prometheus scrapes metrics, Grafana visualizes

## 🎯 Key Features

### Machine Learning
- **Model**: XGBoost Binary Classifier
- **Task**: Network intrusion detection (normal vs attack)
- **Dataset**: UNSW-NB15 (42 features per network flow)
- **Features**: Protocol, service, state, packet counts, byte counts, timing, etc.
- **Real-time Inference**: Sub-second latency per batch

### Distributed Systems
- **Kafka KRaft Mode**: No ZooKeeper dependency
- **Spark Standalone Cluster**: Master + Workers
- **Kubernetes Orchestration**: StatefulSets, Deployments, Services
- **Auto-scaling Ready**: Can integrate KEDA for dynamic scaling

### Monitoring & Observability
- **Kubernetes Metrics**: Pod CPU, memory, disk I/O, restarts
- **Kafka Metrics**: Throughput (msg/sec, bytes/sec), lag, replication health
- **Spark Metrics**: JVM heap, GC stats, executor metrics
- **ServiceMonitors**: Automated Prometheus discovery
- **Grafana Dashboards**: Pre-configured visualizations

## 🚀 Quick Start

### Prerequisites

```bash
# Required tools
- Docker
- Minikube
- kubectl
- Helm (for monitoring)
```

### Installation

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd sys_data

# 2. Start Minikube
make start-minikube

# 3. Deploy Kafka controller
kubectl apply -f apache_kafka/kafka_controller_statefulset.yaml

# 4. Setup complete monitoring stack
make setup-full-monitoring

# Wait for all pods to be ready (check with: kubectl get pods -w)
```

### Running the Pipeline

```bash
# Terminal 1: Start Python Producer
make start_python_producer
# Wait for pod to be ready, then:
make launch_producer
# Keep this running (it continuously produces network events)

# Terminal 2: Submit Spark Job
make submit_spark_job
# This will process events and output predictions
```

### Access Monitoring

```bash
# Terminal 3: Grafana
make access-grafana
# Open http://localhost:3000
# Username: admin, Password: admin

# Terminal 4: Prometheus
make access-prometheus
# Open http://localhost:9090
# Check targets: http://localhost:9090/targets
```

## 📁 Project Structure

```
sys_data/
├── apache_kafka/
│   ├── kafka_broker_statefulset.yaml      # Kafka brokers (KRaft)
│   └── kafka_controller_statefulset.yaml  # Kafka controller
├── spark/
│   ├── spark_master_deployment.yaml       # Spark master
│   ├── spark_worker_deployment.yaml       # Spark workers
│   ├── spark_client_statefulset.yaml      # Job submission client
│   ├── spark_job.py                       # Streaming ML inference job
│   ├── model_utils.py                     # Model loading utilities
│   ├── spark_submit.sh                    # Job submission script
│   └── pretrained_models/
│       ├── xgboost_unsw_nb15_model_binary_class.pkl    # ML model (309KB)
│       ├── label_encoders_binary_class.pkl             # Encoders (2.2KB)
│       ├── xgboost_unsw_nb15_model_multi_class.pkl     # Multi-class (2.8MB)
│       └── label_encoders_multi_class.pkl              # Multi-class encoders
├── python_producer/
│   ├── producer.py                        # Kafka producer (UNSW-NB15)
│   ├── producer_pod.yaml                  # Producer Kubernetes pod
│   └── consumer.py                        # Testing consumer
├── monitoring/
│   ├── servicemonitors.yaml               # Prometheus ServiceMonitors
│   ├── kafka-jmx-config.yaml              # Kafka JMX exporter config
│   ├── kafka-broker-with-metrics.yaml     # Kafka + JMX sidecar
│   ├── spark-metrics-config.yaml          # Spark metrics configuration
│   ├── spark-master-with-metrics.yaml     # Spark master + metrics
│   ├── spark-worker-with-metrics.yaml     # Spark worker + metrics
│   ├── grafana-dashboard-kafka-spark.json # Pre-built dashboard
│   ├── SETUP_GUIDE.md                     # Monitoring setup guide
│   └── METRICS_GUIDE.md                   # Available metrics reference
├── Makefile                                # Command automation
└── README.md                               # This file
```

## 🔧 Makefile Commands

### Basic Pipeline
```bash
make start-minikube           # Start Minikube cluster
make start-kafka              # Deploy Kafka cluster
make start-spark-pods         # Deploy Spark cluster
make start_python_producer    # Deploy producer pod
make launch_producer          # Run producer script
make submit_spark_job         # Submit Spark ML job
make stop-minikube            # Stop Minikube
make delete-resources         # Delete all K8s resources
```

### Monitoring
```bash
make setup-monitoring         # Install Prometheus + Grafana
make access-grafana           # Port-forward to Grafana
make access-prometheus        # Port-forward to Prometheus
make delete-monitoring        # Remove monitoring stack
```

### Advanced Monitoring
```bash
make setup-full-monitoring    # Complete setup (Prometheus + Metrics)
make setup-metrics-exporters  # Deploy ServiceMonitors + ConfigMaps
make deploy-kafka-with-metrics    # Deploy Kafka with JMX exporter
make deploy-spark-with-metrics    # Deploy Spark with Prometheus metrics
```

## 📊 Dataset: UNSW-NB15

The UNSW-NB15 dataset contains network traffic records with 42 features:

### Key Features
- **Network**: proto, service, state, source/dest IPs & ports
- **Packets**: spkts, dpkts (source/destination packet counts)
- **Bytes**: sbytes, dbytes (bytes transferred)
- **Timing**: dur, sttl, dttl, tcprtt, synack, ackdat
- **Performance**: rate, sload, dload, sjit, djit
- **State**: ct_state_ttl, ct_srv_src, ct_dst_ltm, etc.
- **Flags**: is_ftp_login, is_sm_ips_ports

### Labels
- **Binary**: 0 = Normal, 1 = Attack
- **Multi-class**: Normal, Generic, Exploits, Fuzzers, DoS, Reconnaissance, etc.

### Dataset Download
The producer automatically downloads the dataset from Mega.nz on first run.

## 🧠 Machine Learning Pipeline

### Model Architecture
```
Input (42 features)
    ↓
Label Encoding (proto, service, state)
    ↓
XGBoost Binary Classifier
    ↓
Output: 0 (Normal) or 1 (Attack)
```

### Training
Models were pre-trained offline on the full UNSW-NB15 dataset:
- Train/Test split: 80/20
- Hyperparameters: Tuned via cross-validation
- Accuracy: ~85% (binary), ~75% (multi-class)

### Inference
- **Batch Size**: Configurable in Spark (default: micro-batches)
- **Latency**: Sub-second per batch
- **Throughput**: Handles 1000+ events/sec

## 📈 Monitoring & Metrics

### Kubernetes Metrics (Always Available)
```promql
# Pod CPU usage
sum(rate(container_cpu_usage_seconds_total{namespace="default"}[5m])) by (pod)

# Pod memory (MB)
sum(container_memory_working_set_bytes{namespace="default"}) by (pod) / 1024 / 1024

# Pod restarts (critical alert)
kube_pod_container_status_restarts_total{namespace="default"}
```

### Kafka Metrics (After setup-full-monitoring)
```promql
# Messages per second
rate(kafka_server_brokertopicmetrics_messagesinpersec_count[5m])

# Bytes in/out
rate(kafka_server_brokertopicmetrics_bytesinpersec_count[5m])
rate(kafka_server_brokertopicmetrics_bytesoutpersec_count[5m])

# Under-replicated partitions (health check)
kafka_server_replicamanager_underreplicatedpartitions_value
```

### Spark Metrics (After setup-full-monitoring)
```promql
# JVM heap memory
metrics_jvm_heap_used

# GC time
rate(metrics_jvm_PS_MarkSweep_time[5m])
```

See **`monitoring/METRICS_GUIDE.md`** for complete PromQL query reference.

## 🎨 Grafana Dashboards

### Pre-built Dashboard
Import `monitoring/grafana-dashboard-kafka-spark.json`:
1. Go to Grafana → Dashboards → New → Import
2. Upload the JSON file
3. Select Prometheus as data source

### Custom Panels
Create dashboards with:
- **Kafka Throughput**: Events/sec, Bytes/sec
- **Spark Processing**: Batch latency, records processed
- **ML Metrics**: Predictions/sec, Attack rate
- **Resource Usage**: CPU, Memory, Disk I/O
- **Health**: Pod restarts, Under-replicated partitions

## 🐛 Troubleshooting

### Producer Not Producing
```bash
# Check producer logs
kubectl logs python-producer

# Verify Kafka topics
kubectl exec -it kafka-broker-0 -- kafka-topics.sh --list --bootstrap-server localhost:19092

# Check if messages are in topic
kubectl exec -it kafka-broker-0 -- kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic demo \
  --from-beginning \
  --max-messages 5
```

### Spark Job Failing
```bash
# Check Spark job logs
kubectl logs spark-client-0

# Check if models are copied
kubectl exec -it spark-client-0 -- ls -la /opt/spark/work-dir/pretrained_models/

# Check Spark master UI
kubectl port-forward svc/spark-master-service 8080:8080
# Open http://localhost:8080
```

### Monitoring Not Working
```bash
# Check Prometheus targets
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
# Open http://localhost:9090/targets
# All targets should be UP

# Check ServiceMonitors
kubectl get servicemonitor -A

# Check JMX exporter logs (Kafka)
kubectl logs kafka-broker-0 -c jmx-exporter

# Test metrics endpoints
kubectl exec -it kafka-broker-0 -c jmx-exporter -- curl localhost:7071/metrics
```

See **`monitoring/SETUP_GUIDE.md`** for detailed troubleshooting.

## 🔬 Testing

### End-to-End Test
```bash
# 1. Verify Kafka is receiving messages
kubectl exec -it kafka-broker-0 -- kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic demo \
  --max-messages 10

# 2. Check Spark is processing
kubectl logs spark-client-0 | grep "PREDICTION"

# 3. Verify metrics are being collected
# In Prometheus: rate(kafka_server_brokertopicmetrics_messagesinpersec_count[1m])
```

### Unit Testing
```bash
# Test producer locally
cd python_producer
python3 producer.py  # Requires local Kafka

# Test model loading
cd spark
python3 -c "from model_utils import load_model; print(load_model('label_encoders_binary_class.pkl'))"
```

## 🚧 Future Enhancements

### Short Term
- [ ] Add KEDA for auto-scaling based on Kafka lag
- [ ] Persist predictions to MongoDB or PostgreSQL
- [ ] Add REST API to query attack predictions
- [ ] Configure Grafana alerting (Slack, Email)
- [ ] Add consumer lag exporter

### Long Term
- [ ] Real-time dashboard showing live attacks on a map
- [ ] A/B testing for multiple ML models
- [ ] Feature drift detection
- [ ] Model retraining pipeline
- [ ] Multi-cloud deployment (AWS EKS, GCP GKE, Azure AKS)

## 📚 References

### Inspiration
- [k8s-data-processing](https://github.com/medyassineakhmari/k8s-data-processing) - Monitoring architecture reference

### Technologies
- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine
- [XGBoost](https://xgboost.readthedocs.io/) - Gradient boosting library
- [Prometheus](https://prometheus.io/) - Monitoring system
- [Grafana](https://grafana.com/) - Observability platform
- [Kubernetes](https://kubernetes.io/) - Container orchestration

### Dataset
- [UNSW-NB15](https://research.unsw.edu.au/projects/unsw-nb15-dataset) - Network intrusion dataset

## 👥 Authors

- **Wail Yacoubi** - Monitoring setup and integration
- **Original Team** - ML pipeline and Kafka/Spark architecture

## 📄 License

This project is for educational purposes (distributed systems + machine learning course).

## 🙏 Acknowledgments

- UNSW for providing the UNSW-NB15 dataset
- Prometheus community for kube-prometheus-stack
- Bitnami for JMX Exporter Docker images
- k8s-data-processing project for monitoring patterns
