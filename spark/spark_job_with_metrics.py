"""
Spark Streaming Job with Prometheus Metrics
Detects cyber attacks in real-time using XGBoost ML model
Exposes metrics on port 8000 for Prometheus scraping
"""

import joblib
import pandas as pd
import sklearn
import xgboost
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, struct
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, FloatType, StructField

from model_utils_with_metrics import BinaryClassificationModel, load_model

# ============================================
# PROMETHEUS METRICS SETUP
# ============================================
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import threading

# Start Prometheus HTTP server on port 8000
def start_metrics_server():
    try:
        start_http_server(8000)
        print("✅ Prometheus metrics server started on port 8000")
    except Exception as e:
        print(f"⚠️  Warning: Could not start metrics server: {e}")

# Start in background thread
threading.Thread(target=start_metrics_server, daemon=True).start()

# ============================================
# METRIC DEFINITIONS (Top 15 Essential Metrics)
# ============================================

# 1. Security Metrics (CRITICAL)
predictions_counter = Counter(
    'spark_predictions_total',
    'Total predictions made by ML model',
    ['prediction']  # Labels: malicious, benign
)

attack_types_counter = Counter(
    'spark_attacks_by_type_total',
    'Attacks detected by type (UNSW-NB15 categories)',
    ['attack_type']
)

malicious_rate = Gauge(
    'spark_malicious_rate_per_second',
    'Current rate of malicious predictions per second'
)

# 2. Performance Metrics
kafka_consumer_lag = Gauge(
    'spark_kafka_consumer_lag_records',
    'Number of records Spark is behind Kafka',
    ['topic', 'partition']
)

batch_duration = Histogram(
    'spark_streaming_batch_duration_seconds',
    'Time taken to process each micro-batch',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

e2e_latency = Histogram(
    'spark_e2e_latency_seconds',
    'End-to-end latency from Kafka to prediction',
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# 3. ML Model Metrics
model_inference_duration = Histogram(
    'spark_model_inference_duration_seconds',
    'Time taken for ML model inference',
    buckets=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0]
)

model_predictions_rate = Gauge(
    'spark_model_predictions_per_second',
    'Current ML prediction throughput'
)

model_errors = Counter(
    'spark_model_errors_total',
    'Total ML model inference errors',
    ['error_type']
)

# 4. Data Quality Metrics
records_processed = Counter(
    'spark_records_processed_total',
    'Total records processed',
    ['status']  # Labels: valid, invalid
)

parsing_errors = Counter(
    'spark_parsing_errors_total',
    'JSON parsing errors',
    ['error_type']
)

# 5. System Health Metrics
active_batches = Gauge(
    'spark_streaming_active_batches',
    'Number of batches currently being processed'
)

executor_memory_usage = Gauge(
    'spark_executor_memory_used_mb',
    'Executor memory usage in MB'
)

# 6. Network Traffic Metrics
attacks_by_protocol = Counter(
    'spark_attacks_by_protocol_total',
    'Malicious traffic by protocol',
    ['protocol', 'prediction']
)

attacks_by_service = Counter(
    'spark_attacks_by_service_total',
    'Malicious traffic by service',
    ['service', 'prediction']
)

# ============================================
# SCHEMA DEFINITION (41 features)
# ============================================
fields = [
    ("proto", StringType()),
    ("state", StringType()),
    ("dur", FloatType()),
    ("sbytes", IntegerType()),
    ("dbytes", IntegerType()),
    ("sttl", IntegerType()),
    ("dttl", IntegerType()),
    ("sloss", IntegerType()),
    ("dloss", IntegerType()),
    ("service", StringType()),
    ("sload", FloatType()),
    ("dload", FloatType()),
    ("spkts", IntegerType()),
    ("dpkts", IntegerType()),
    ("swin", IntegerType()),
    ("dwin", IntegerType()),
    ("stcpb", IntegerType()),
    ("dtcpb", IntegerType()),
    ("smean", IntegerType()),
    ("dmean", IntegerType()),
    ("trans_depth", IntegerType()),
    ("response_body_len", IntegerType()),
    ("sjit", FloatType()),
    ("djit", FloatType()),
    ("sinpkt", FloatType()),
    ("dinpkt", FloatType()),
    ("tcprtt", FloatType()),
    ("synack", FloatType()),
    ("ackdat", FloatType()),
    ("is_sm_ips_ports", IntegerType()),
    ("ct_state_ttl", IntegerType()),
    ("ct_flw_http_mthd", IntegerType()),
    ("is_ftp_login", IntegerType()),
    ("ct_ftp_cmd", IntegerType()),
    ("ct_srv_src", IntegerType()),
    ("ct_srv_dst", IntegerType()),
    ("ct_dst_ltm", IntegerType()),
    ("ct_src_ltm", IntegerType()),
    ("ct_src_dport_ltm", IntegerType()),
    ("ct_dst_sport_ltm", IntegerType()),
    ("ct_dst_src_ltm", IntegerType()),
]
schema = StructType([StructField(name, dtype, True) for name, dtype in fields])

# ============================================
# ATTACK TYPE MAPPING (UNSW-NB15 Dataset)
# ============================================
# Since producer drops 'attack_cat' column, we can't track attack types in real-time
# This would require keeping the label in Kafka messages
# For now, we'll track protocol/service-based patterns

# ============================================
# MAIN STREAMING APPLICATION
# ============================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting Cyber Attack Detection System with Metrics")
    print("=" * 60)

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("CyberAttackDetectionWithMetrics")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    topics_name = "demo"

    print(f"📡 Connecting to Kafka topic: {topics_name}")

    # Read from Kafka
    logs_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-broker-0.kafka-broker-service:19092")
        .option("subscribe", topics_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON messages
    logs_df = logs_raw.select(
        col("timestamp").alias("kafka_timestamp"),  # Keep Kafka timestamp for latency calculation
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_timestamp", "data.*")

    logs_df = logs_df.fillna(0)

    print("🤖 Loading ML models...")
    encoder_model = load_model("label_encoders_binary_class.pkl")
    classifier_model = load_model("xgboost_unsw_nb15_model_binary_class.pkl")

    classification_model = BinaryClassificationModel(
        encoder_model,
        classifier_model,
        model_inference_duration  # Pass metric to model
    )
    broadcasted_model = spark.sparkContext.broadcast(classification_model)

    print("✅ Models loaded and broadcasted")

    # Pandas UDF for predictions
    @pandas_udf(returnType=IntegerType())
    def predict_with_model(X_values_batch: pd.DataFrame) -> pd.Series:
        model = broadcasted_model.value
        predictions = model.predict(X_values_batch)

        if predictions is None:
            # Track model errors
            model_errors.labels(error_type='inference_failed').inc(len(X_values_batch))
            return pd.Series([None] * len(X_values_batch))
        else:
            return pd.Series(predictions)

    # Add predictions
    predictions_df = logs_df.withColumn(
        "label",
        predict_with_model(struct(*[col(c) for c, dtype in fields]))
    )

    # ============================================
    # METRICS COLLECTION via foreachBatch
    # ============================================

    def process_batch_with_metrics(batch_df, batch_id):
        """
        Process each micro-batch and collect metrics
        """
        batch_start_time = time.time()

        try:
            # Increment active batches
            active_batches.inc()

            # Collect batch to process metrics
            batch_records = batch_df.collect()
            total_records = len(batch_records)

            if total_records == 0:
                print(f"⏭️  Batch {batch_id}: No records")
                return

            malicious_count = 0
            benign_count = 0
            proto_counts = {}
            service_counts = {}

            # Process records
            for row in batch_records:
                label = row['label']
                proto = row['proto'] if row['proto'] else 'unknown'
                service = row['service'] if row['service'] else 'unknown'

                # Count predictions
                if label == 1:
                    malicious_count += 1
                    predictions_counter.labels(prediction='malicious').inc()
                    attacks_by_protocol.labels(protocol=proto, prediction='malicious').inc()
                    attacks_by_service.labels(service=service, prediction='malicious').inc()
                elif label == 0:
                    benign_count += 1
                    predictions_counter.labels(prediction='benign').inc()
                    attacks_by_protocol.labels(protocol=proto, prediction='benign').inc()
                    attacks_by_service.labels(service=service, prediction='benign').inc()
                else:
                    # Invalid prediction (None)
                    records_processed.labels(status='invalid').inc()
                    continue

                # Track valid records
                records_processed.labels(status='valid').inc()

            # Calculate batch metrics
            batch_elapsed = time.time() - batch_start_time
            batch_duration.observe(batch_elapsed)

            # Calculate rates
            if batch_elapsed > 0:
                malicious_rate.set(malicious_count / batch_elapsed)
                model_predictions_rate.set(total_records / batch_elapsed)

            # Decrement active batches
            active_batches.dec()

            # Print batch summary
            malicious_pct = (malicious_count / total_records * 100) if total_records > 0 else 0
            print(f"📊 Batch {batch_id}: {total_records} records | "
                  f"🔴 Malicious: {malicious_count} ({malicious_pct:.1f}%) | "
                  f"🟢 Benign: {benign_count} | "
                  f"⏱️  {batch_elapsed:.2f}s")

            # Also write to console for debugging
            batch_df.show(10, truncate=False)

        except Exception as e:
            print(f"❌ Error processing batch {batch_id}: {e}")
            parsing_errors.labels(error_type='batch_processing_error').inc()
            active_batches.dec()

    # ============================================
    # START STREAMING QUERY
    # ============================================

    print("🎯 Starting streaming query with metrics collection...")
    print(f"📈 Metrics endpoint: http://spark-client-0:8000/metrics")
    print("=" * 60)

    query = (
        predictions_df.writeStream
        .foreachBatch(process_batch_with_metrics)
        .option("checkpointLocation", "/tmp/checkpoints/logs-stream")
        .start()
    )

    print("✅ Streaming query started successfully")
    print("⏳ Waiting for data... (Press Ctrl+C to stop)")

    query.awaitTermination()
