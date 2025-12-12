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