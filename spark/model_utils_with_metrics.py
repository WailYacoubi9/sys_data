"""
ML Model utilities with Prometheus metrics integration
Wraps XGBoost model with performance tracking
"""

import joblib
import pandas as pd
import sklearn
import xgboost
import time


def load_model(name):
    """Load a pre-trained model from disk"""
    return joblib.load(f"./pretrained_models/{name}")


class BinaryClassificationModel:
    """
    XGBoost binary classifier for cyber attack detection
    Tracks inference latency via Prometheus metrics
    """

    def __init__(self, encoder_model, classifier_model, inference_metric=None):
        self.label_encoders = encoder_model
        self.classifier = classifier_model
        self.inference_metric = inference_metric  # Prometheus Histogram

    def predict(self, X_values):
        """
        Predict labels for input data

        Args:
            X_values: pandas DataFrame with network flow features

        Returns:
            numpy array of predictions (0=benign, 1=malicious) or None on error
        """
        inference_start = time.time()

        try:
            categorical_cols_binary = ['proto', 'service', 'state']
            label_encoders_binary_import = self.label_encoders

            # Encode categorical columns
            for col in categorical_cols_binary:
                le_binary = label_encoders_binary_import[col]
                # Map unseen test values to 'Unknown' before transforming
                X_values[col] = X_values[col].apply(
                    lambda x: x if x in le_binary.classes_ else 'Unknown'
                )
                X_values[col] = le_binary.transform(X_values[col])

            # ML Inference
            y_values_pred = self.classifier.predict(X_values)

            # Track inference latency
            if self.inference_metric is not None:
                inference_elapsed = time.time() - inference_start
                self.inference_metric.observe(inference_elapsed)

            # Debug output
            malicious_count = sum(y_values_pred == 1)
            benign_count = sum(y_values_pred == 0)
            print(f"🤖 ML Inference: {len(y_values_pred)} records | "
                  f"Malicious: {malicious_count} | Benign: {benign_count}")

            return y_values_pred

        except ValueError as e:
            print(f"❌ ML Inference FAILED: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error during inference: {e}")
            return None
