# producer/producer.py
"""
Kafka Producer: reads COCO128 images, encodes them as base64,
and publishes each frame to the 'raw-frames' Kafka topic.

GOAL: simulate a camera stream by reading images and publishing them to kafka as events.

Kafka is a distributed event streaming platform that transports image frames from producers to consumers. aka data bus.
    - Benefits: Decouples systems
                Buffers traffic spikes. The producer can keep sending frames while consumers process at their own speed.
                Allows multiple consumers
                Provides fault tolerance
    - If camera crashes, Kafka still has the data.
    - Kafka sits between the producer (camera) and consumer (spark structured streaming)
    - Visualization: Images --> Producer --> Kafka --> Consumer --> PostgreSQL
                                                    |
                                                    +--> Spark
                                                    +--> YOLO
                                                    +--> MLflow

"""

import os
import json
import base64
import time
import uuid
from pathlib import Path
from kafka import KafkaProducer
from PIL import Image
import io

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-frames")
IMAGE_DIR = Path("data/coco128/images/train2017")
DELAY_SECONDS = 0.5  # simulate a 2fps stream


def encode_image(image_path: Path) -> str:
    """Resize image to 640x640 and return base64-encoded JPEG string."""
    with Image.open(image_path) as img:
        img = img.convert("RGB").resize((640, 640))
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=85)
        return base64.b64encode(buffer.getvalue()).decode("utf-8")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10485760,  # 10MB — images can be large
    )

    images = list(IMAGE_DIR.glob("*.jpg"))
    print(f"[Producer] Found {len(images)} images. Publishing to topic '{KAFKA_TOPIC}'...")

    for i, img_path in enumerate(images):
        message = {
            "frame_id": str(uuid.uuid4()),
            "filename": img_path.name,
            "timestamp": time.time(),
            "stream": "coco128-stream",
            "image_b64": encode_image(img_path),
        }
        producer.send(KAFKA_TOPIC, value=message)
        print(f"[Producer] Sent frame {i+1}/{len(images)}: {img_path.name}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print("[Producer] Done.")


if __name__ == "__main__":
    main()