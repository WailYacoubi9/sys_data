# this makefile may not work on Windows
# execute in this order to start the full pipeline:

start-minikube:
	minikube start --driver=docker --memory=4096 --cpus=2

# make take a while to download the images and start the pods
start-kafka:
	kubectl apply -f apache_kafka/kafka_broker_statefulset.yaml
	kubectl apply -f apache_kafka/kafka_controller_statefulset.yaml

# make take a while to download the images and start the pods
start-spark-pods:
	kubectl apply -f spark/spark_master_deployment.yaml
	kubectl apply -f spark/spark_worker_deployment.yaml
	kubectl apply -f spark/spark_client_statefulset.yaml

start_python_producer:
	kubectl apply -f python_producer/producer_pod.yaml

launch_producer:
	#copy the producer.py into the producer pod
	kubectl cp python_producer/producer.py python-producer:/producer.py
	#execute the producer.py inside the producer pod
	# keep it running to continuously produce messages to Kafka
	kubectl exec -it python-producer -- python3 /producer.py

launch_producer_alt:
	# Alternative method using stdin to avoid kubectl cp timeouts
	@echo "Copying producer.py via stdin..."
	@cat python_producer/producer.py | kubectl exec -i python-producer -- sh -c 'cat > /producer.py'
	@echo "Launching producer..."
	kubectl exec -it python-producer -- python3 /producer.py

submit_spark_job:
	#copy the spark_job.py into the spark-client pod
	kubectl cp spark/spark_job.py spark-client-0:/opt/spark/work-dir/spark_job.py
	kubectl cp spark/model_utils.py spark-client-0:/opt/spark/work-dir/model_utils.py
	kubectl cp spark/pretrained_models/ spark-client-0:/opt/spark/work-dir/pretrained_models/
	kubectl cp spark/spark_submit.sh spark-client-0:/opt/spark/work-dir/spark_submit.sh
	#execute the spark_job.py inside the spark-client pod
	#may take a while to execute (download dependencies, connect to master, dag scheduling, etc)
	# it will process new message as they arrive in Kafka topic
	# if no more message, it will just wait for new messages
	kubectl exec spark-client-0 -- /bin/bash /opt/spark/work-dir/spark_submit.sh

submit_spark_job_alt:
	# Alternative method using tar + stdin to avoid kubectl cp timeouts
	@echo "Copying Spark files via tar+stdin..."
	@cd spark && tar czf - spark_job.py model_utils.py spark_submit.sh pretrained_models/ | \
		kubectl exec -i spark-client-0 -- tar xzf - -C /opt/spark/work-dir/
	@echo "Submitting Spark job..."
	kubectl exec spark-client-0 -- /bin/bash /opt/spark/work-dir/spark_submit.sh

stop-minikube:
	minikube stop

delete-resources:
	kubectl delete all --all

# ============================================
# MONITORING TARGETS (Added by Wail)
# ============================================

setup-monitoring:
	kubectl create namespace monitoring || echo "Namespace already exists"
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts || echo "Repo already added"
	helm repo update
	helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack --namespace monitoring --set prometheus.prometheusSpec.retention=30d --set grafana.adminPassword=admin --set grafana.service.type=NodePort --wait --timeout=10m
	@echo "=========================================="
	@echo "Monitoring deployed successfully!"
	@echo "Access Grafana: make access-grafana"
	@echo "Then open: http://localhost:3000"
	@echo "Username: admin | Password: admin"
	@echo "=========================================="

access-grafana:
	@echo "Opening Grafana at http://localhost:3000"
	@echo "Username: admin | Password: admin"
	kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80

access-prometheus:
	@echo "Opening Prometheus at http://localhost:9090"
	kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090

delete-monitoring:
	helm uninstall kube-prometheus-stack -n monitoring || echo "Stack not found"
	kubectl delete namespace monitoring || echo "Namespace not found"

# ============================================
# KAFKA MONITORING TARGETS
# ============================================

setup-kafka-monitoring:
	@echo "📊 Setting up Kafka monitoring with JMX Exporter..."
	@echo "Step 1: Creating JMX Exporter ConfigMap..."
	kubectl apply -f apache_kafka/kafka-jmx-exporter-config.yaml
	@echo "Step 2: Updating Kafka broker StatefulSet..."
	kubectl apply -f apache_kafka/kafka_broker_statefulset.yaml
	@echo "Step 3: Restarting Kafka brokers..."
	kubectl rollout restart statefulset/kafka-broker
	@echo "⏳ Waiting for brokers to restart (this may take 1-2 minutes)..."
	kubectl wait --for=condition=ready pod -l app=kafka-broker --timeout=300s || echo "Warning: Timeout waiting for pods, check manually"
	@echo "Step 4: Creating ServiceMonitor for Prometheus..."
	kubectl apply -f apache_kafka/kafka-servicemonitor.yaml
	@echo "=========================================="
	@echo "✅ Kafka monitoring configured successfully!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Wait 1-2 minutes for Prometheus to start scraping"
	@echo "  2. Verify metrics: make verify-kafka-metrics"
	@echo "  3. Import dashboard: make import-grafana-dashboard"
	@echo "  4. Access Grafana: make access-grafana"
	@echo "=========================================="

verify-kafka-metrics:
	@echo "🔍 Verifying Kafka metrics are being scraped..."
	@echo ""
	@echo "=== ServiceMonitor Status ==="
	kubectl get servicemonitor -n monitoring kafka-broker-metrics || echo "❌ ServiceMonitor not found!"
	@echo ""
	@echo "=== Kafka Broker Pods ==="
	kubectl get pods -l app=kafka-broker
	@echo ""
	@echo "=== Metrics Endpoints ==="
	@echo "Broker pods and their metrics endpoints:"
	@kubectl get pods -l app=kafka-broker -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.podIP}:5556/metrics{"\n"}{end}'
	@echo ""
	@echo "=== Testing Metrics from kafka-broker-0 ==="
	@kubectl exec kafka-broker-0 -- wget -qO- localhost:5556/metrics 2>/dev/null | grep kafka_log_log_size | head -5 || echo "❌ No metrics found - check JMX exporter logs"
	@echo ""
	@echo "=== JMX Exporter Files Check ==="
	@kubectl exec kafka-broker-0 -- ls -la /opt/jmx-exporter/ 2>/dev/null || echo "❌ JMX exporter directory not found"
	@echo ""
	@echo "✅ Verification complete. If you see kafka_log_log_size metrics above, monitoring is working!"
	@echo "   Next: Check Prometheus targets at http://localhost:9090/targets (run 'make access-prometheus')"

import-grafana-dashboard:
	@echo "📈 Importing Kafka dashboard to Grafana..."
	@echo "Creating ConfigMap with dashboard JSON..."
	kubectl create configmap kafka-dashboard -n monitoring --from-file=monitoring/kafka-messages-dashboard.json --dry-run=client -o yaml | kubectl apply -f -
	@echo "✅ Dashboard ConfigMap created!"
	@echo ""
	@echo "To import in Grafana UI:"
	@echo "  1. Run: make access-grafana"
	@echo "  2. Open http://localhost:3000 (admin/admin)"
	@echo "  3. Go to Dashboards → Import"
	@echo "  4. Click 'Upload JSON file'"
	@echo "  5. Select: monitoring/kafka-messages-dashboard.json"
	@echo "  6. Click 'Import'"
	@echo ""
	@echo "Or manually: Copy the JSON from monitoring/kafka-messages-dashboard.json"

test-kafka-metrics-query:
	@echo "🧪 Testing Prometheus queries for Kafka metrics..."
	@echo "Port-forwarding to Prometheus (if not already running)..."
	@echo "You can test these queries in Prometheus UI (http://localhost:9090):"
	@echo ""
	@echo "1. Total messages in topic 'demo':"
	@echo "   sum(kafka_log_logendoffset{topic=\"demo\"} - kafka_log_logstartoffset{topic=\"demo\"})"
	@echo ""
	@echo "2. Messages per partition:"
	@echo "   kafka_log_logendoffset{topic=\"demo\"} - kafka_log_logstartoffset{topic=\"demo\"}"
	@echo ""
	@echo "3. Message ingestion rate:"
	@echo "   rate(kafka_server_topic_messages_in_total{topic=\"demo\"}[1m])"
	@echo ""
	@echo "4. Broker JVM heap memory:"
	@echo "   jvm_memory_heap_used{job=\"kafka-broker-metrics\"}"
	@echo ""
	@echo "Run 'make access-prometheus' to open Prometheus UI"
# ============================================
# SPARK METRICS TARGETS (ML Attack Detection)
# ============================================

setup-spark-metrics:
	@echo "📊 Setting up Spark metrics monitoring..."
	@echo "Step 1: Installing prometheus_client in Spark pod..."
	kubectl exec spark-client-0 -- pip install prometheus_client || echo "Already installed"
	@echo "Step 2: Creating ServiceMonitor for Spark metrics..."
	kubectl apply -f spark/spark-metrics-servicemonitor.yaml
	@echo "Step 3: Verifying service..."
	kubectl get svc spark-client-metrics || echo "Service will be created after job starts"
	@echo "=========================================="
	@echo "✅ Spark metrics setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Deploy Spark job with metrics: make deploy-spark-job-with-metrics"
	@echo "  2. Wait 30 seconds for Prometheus to scrape"
	@echo "  3. Verify metrics: make verify-spark-metrics"
	@echo "  4. Import dashboards: make import-spark-dashboards"
	@echo "=========================================="

deploy-spark-job-with-metrics:
	@echo "🚀 Deploying Spark job with Prometheus metrics..."
	@echo "Copying files to spark-client pod..."
	kubectl cp spark/spark_job_with_metrics.py spark-client-0:/opt/spark/work-dir/spark_job.py
	kubectl cp spark/model_utils_with_metrics.py spark-client-0:/opt/spark/work-dir/model_utils.py
	kubectl cp spark/pretrained_models/ spark-client-0:/opt/spark/work-dir/pretrained_models/
	kubectl cp spark/spark_submit.sh spark-client-0:/opt/spark/work-dir/spark_submit.sh
	@echo "Installing prometheus_client..."
	kubectl exec spark-client-0 -- pip install prometheus_client
	@echo "✅ Files deployed! Ready to submit job."
	@echo ""
	@echo "To start the job, run:"
	@echo "  kubectl exec spark-client-0 -- /bin/bash /opt/spark/work-dir/spark_submit.sh"

verify-spark-metrics:
	@echo "🔍 Verifying Spark metrics..."
	@echo ""
	@echo "=== ServiceMonitor Status ==="
	kubectl get servicemonitor -n monitoring spark-client-metrics || echo "❌ ServiceMonitor not found!"
	@echo ""
	@echo "=== Spark Client Service ==="
	kubectl get svc spark-client-metrics || echo "⚠️  Service not found - job may not be running"
	@echo ""
	@echo "=== Testing Metrics Endpoint ==="
	@echo "Attempting to fetch metrics from spark-client-0..."
	@kubectl exec spark-client-0 -- wget -qO- localhost:8000/metrics 2>/dev/null | grep spark_predictions_total || echo "⚠️  No metrics yet - is the Spark job running?"
	@echo ""
	@echo "=== Sample Metrics to Check ==="
	@kubectl exec spark-client-0 -- wget -qO- localhost:8000/metrics 2>/dev/null | grep -E "spark_predictions_total|spark_malicious_rate|spark_model_inference" | head -10 || echo "No metrics available yet"
	@echo ""
	@echo "✅ If you see metrics above, monitoring is working!"
	@echo "   Next: Check Prometheus targets at http://localhost:9090/targets"
	@echo "   Run: make access-prometheus"

import-spark-dashboards:
	@echo "📈 Importing Spark metrics dashboards to Grafana..."
	@echo ""
	@echo "Dashboard 1: Security Overview (Attack Detection)"
	kubectl create configmap security-overview-dashboard -n monitoring \
		--from-file=monitoring/security-overview-dashboard.json \
		--dry-run=client -o yaml | kubectl apply -f -
	@echo "✅ Security Overview dashboard created"
	@echo ""
	@echo "Dashboard 2: System Health (Performance Monitoring)"
	kubectl create configmap system-health-dashboard -n monitoring \
		--from-file=monitoring/system-health-dashboard.json \
		--dry-run=client -o yaml | kubectl apply -f -
	@echo "✅ System Health dashboard created"
	@echo ""
	@echo "=========================================="
	@echo "📊 To import dashboards in Grafana UI:"
	@echo "  1. Run: make access-grafana"
	@echo "  2. Open http://localhost:3000 (admin/admin)"
	@echo "  3. Go to: Dashboards → Import"
	@echo "  4. Click: Upload JSON file"
	@echo "  5. Select: monitoring/security-overview-dashboard.json"
	@echo "  6. Select: monitoring/system-health-dashboard.json"
	@echo "  7. Click: Import"
	@echo ""
	@echo "Or use the JSON directly from the ConfigMaps created above"
	@echo "=========================================="

test-spark-metrics-queries:
	@echo "🧪 Testing Prometheus queries for Spark metrics..."
	@echo ""
	@echo "Test these queries in Prometheus UI (http://localhost:9090):"
	@echo ""
	@echo "1. Total malicious predictions:"
	@echo "   spark_predictions_total{prediction=\"malicious\"}"
	@echo ""
	@echo "2. Attack rate per second:"
	@echo "   rate(spark_predictions_total{prediction=\"malicious\"}[1m])"
	@echo ""
	@echo "3. Malicious traffic percentage:"
	@echo "   (spark_predictions_total{prediction=\"malicious\"} / "
	@echo "    (spark_predictions_total{prediction=\"malicious\"} + "
	@echo "     spark_predictions_total{prediction=\"benign\"})) * 100"
	@echo ""
	@echo "4. ML model inference P99 latency:"
	@echo "   histogram_quantile(0.99, spark_model_inference_duration_seconds_bucket)"
	@echo ""
	@echo "5. Kafka consumer lag:"
	@echo "   spark_kafka_consumer_lag_records"
	@echo ""
	@echo "6. Batch processing duration P95:"
	@echo "   histogram_quantile(0.95, spark_streaming_batch_duration_seconds_bucket)"
	@echo ""
	@echo "Run 'make access-prometheus' to open Prometheus UI"

monitor-attacks-live:
	@echo "🔴 Monitoring live attack detection..."
	@echo "Press Ctrl+C to stop"
	@echo "=========================================="
	@while true; do \
		kubectl exec spark-client-0 -- wget -qO- localhost:8000/metrics 2>/dev/null | \
		grep -E "spark_predictions_total|spark_malicious_rate" | \
		awk '{print strftime("%H:%M:%S"), $$0}'; \
		sleep 5; \
	done

# ============================================
# COMPLETE MONITORING SETUP (ALL-IN-ONE)
# ============================================

setup-complete-monitoring:
	@echo "🚀 Setting up COMPLETE monitoring stack..."
	@echo "This will setup: Prometheus, Grafana, Kafka metrics, Spark metrics"
	@echo "=========================================="
	@echo ""
	@echo "Step 1/4: Setting up Prometheus & Grafana..."
	@$(MAKE) setup-monitoring
	@echo ""
	@echo "Step 2/4: Setting up Kafka monitoring..."
	@$(MAKE) setup-kafka-monitoring
	@echo ""
	@echo "Step 3/4: Setting up Spark metrics..."
	@$(MAKE) setup-spark-metrics
	@echo ""
	@echo "Step 4/4: Importing all dashboards..."
	@$(MAKE) import-grafana-dashboard
	@$(MAKE) import-spark-dashboards
	@echo ""
	@echo "=========================================="
	@echo "✅ COMPLETE MONITORING STACK READY!"
	@echo ""
	@echo "Access points:"
	@echo "  • Grafana:    make access-grafana (http://localhost:3000)"
	@echo "  • Prometheus: make access-prometheus (http://localhost:9090)"
	@echo ""
	@echo "Available dashboards:"
	@echo "  1. Kafka Messages Monitoring"
	@echo "  2. Security Overview (Attack Detection)"
	@echo "  3. System Health (Spark Performance)"
	@echo ""
	@echo "Next: Deploy Spark job with metrics"
	@echo "  make deploy-spark-job-with-metrics"
	@echo "=========================================="

# ============================================
# HELPFUL ALIASES
# ============================================

metrics-help:
	@echo "📊 Spark Metrics Monitoring - Quick Reference"
	@echo "=========================================="
	@echo ""
	@echo "Setup Commands:"
	@echo "  make setup-spark-metrics          - Configure Spark metrics monitoring"
	@echo "  make deploy-spark-job-with-metrics - Deploy Spark job with metrics"
	@echo "  make setup-complete-monitoring    - Setup everything (Kafka + Spark)"
	@echo ""
	@echo "Verification Commands:"
	@echo "  make verify-spark-metrics         - Check if metrics are working"
	@echo "  make test-spark-metrics-queries   - Show example Prometheus queries"
	@echo "  make monitor-attacks-live         - Watch attack detection in real-time"
	@echo ""
	@echo "Dashboard Commands:"
	@echo "  make import-spark-dashboards      - Import Grafana dashboards"
	@echo "  make access-grafana               - Open Grafana UI"
	@echo "  make access-prometheus            - Open Prometheus UI"
	@echo ""
	@echo "Documentation:"
	@echo "  See METRICS_IMPLEMENTATION_GUIDE.md for full guide"
	@echo ""
	@echo "=========================================="

