# consumer/consumer.py
"""
PySpark Structured Streaming consumer.
- Reads messages from Kafka topic 'raw-frames'
- Decodes base64 image
- Runs YOLOv5n object detection
- Writes detection results to PostgreSQL
- Logs metrics to MLflow
"""

import os
import json
import base64
import time
import io
import uuid
from datetime import datetime

import torch
import numpy as np
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine, text
import mlflow

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "raw-frames")
PG_USER         = os.getenv("POSTGRES_USER", "postgres")
PG_PASS         = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_HOST         = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT         = os.getenv("POSTGRES_PORT", "5432")
PG_DB           = os.getenv("POSTGRES_DB", "detections_db")
MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5001")
MODEL_PATH      = os.getenv("MODEL_PATH", "consumer/yolov5n.pt")
PG_CONN_STR     = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Windows: Spark checkpoint must use forward slashes even on Windows
CHECKPOINT_DIR  = "C:/tmp/spark-checkpoint"

# ── Load model once at module level ─────────────────────────────────────────
print("[Consumer] Loading YOLOv5n model...")
model = torch.hub.load("ultralytics/yolov5", "custom", path=MODEL_PATH, verbose=False)
model.conf = 0.4   # only report detections with >= 40% confidence
model.iou  = 0.45  # IoU threshold for non-maximum suppression
model.eval()
print("[Consumer] Model loaded.")

# ── MLflow setup ─────────────────────────────────────────────────────────────
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("vision-inference-pipeline")


# Create database engine once at module level
engine = create_engine(PG_CONN_STR, pool_size=10, max_overflow=20)


def run_inference(image_b64: str):
    """Decode base64 image, run YOLO, return list of detection dicts."""
    img_bytes = base64.b64decode(image_b64)
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    img_np = np.array(img)

    start = time.time()
    results = model(img_np)
    latency_ms = (time.time() - start) * 1000

    detections = []
    
    for *box, conf, cls in results.xyxy[0].tolist():
        x1, y1, x2, y2 = box
        detections.append({
            "class_name": model.names[int(cls)],
            "confidence": round(conf, 4),
            "bbox_x": round(x1, 2),
            "bbox_y": round(y1, 2),
            "bbox_w": round(x2 - x1, 2),
            "bbox_h": round(y2 - y1, 2),
        })

    return detections, latency_ms


def write_to_postgres(batch_df, batch_id):
    """Write a micro-batch of detections to PostgreSQL."""
    if batch_df.isEmpty():
        return

    rows_written = 0
    total_latency = 0

    with engine.begin() as conn:
        for row in batch_df.collect():
            try:
                msg = json.loads(row["value"])
                frame_id    = msg.get("frame_id", str(uuid.uuid4()))
                stream      = msg.get("stream", "unknown")
                image_b64   = msg.get("image_b64", "")
                detected_at = datetime.fromtimestamp(msg.get("timestamp", time.time()))

                detections, latency_ms = run_inference(image_b64)
                total_latency += latency_ms

                for det in detections:
                    conn.execute(text("""
                        INSERT INTO detection_events
                            (frame_id, detected_at, class_id, source_id, confidence,
                             bbox_x, bbox_y, bbox_w, bbox_h)
                        VALUES (
                            :frame_id, :detected_at,
                            (SELECT class_id FROM dim_class WHERE class_name = :class_name),
                            (SELECT source_id FROM dim_source WHERE stream_name = :stream),
                            :confidence, :bbox_x, :bbox_y, :bbox_w, :bbox_h
                        )
                    """), {
                        "frame_id":    frame_id,
                        "detected_at": detected_at,
                        "class_name":  det["class_name"],
                        "stream":      stream,
                        "confidence":  det["confidence"],
                        "bbox_x":      det["bbox_x"],
                        "bbox_y":      det["bbox_y"],
                        "bbox_w":      det["bbox_w"],
                        "bbox_h":      det["bbox_h"],
                    })
                    rows_written += 1

                print(f"[Consumer] Batch {batch_id} | frame={frame_id} | "
                      f"detections={len(detections)} | latency={latency_ms:.1f}ms")

            except Exception as e:
                print(f"[Consumer] Error processing row: {e}")

    with mlflow.start_run(run_name=f"batch_{batch_id}", nested=True):
        mlflow.log_metric("rows_written", rows_written)
        if rows_written > 0:
            mlflow.log_metric("avg_latency_ms", total_latency / (batch_id + 1))
        mlflow.log_param("model", "yolov5n")
        mlflow.log_param("confidence_threshold", 0.4)


def main():
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    spark = (SparkSession.builder
             .appName("VisionInferencePipeline")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                  .option("subscribe", KAFKA_TOPIC)
                  .option("startingOffsets", "latest")
                  .load()
                  .selectExpr("CAST(value AS STRING) as value"))

    query = (raw_stream.writeStream
             .foreachBatch(write_to_postgres)
             .trigger(processingTime="10 seconds")
             .start())

    print("[Consumer] Streaming started. Waiting for frames...")
    query.awaitTermination()


if __name__ == "__main__":
    main()