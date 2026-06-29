# Vision Inference Pipeline

A local end-to-end computer vision MLOps pipeline that uses Docker, Kafka, PySpark, PostgreSQL, Airflow, and MLflow.

## Project overview

This repository contains a prototype pipeline that simulates live image ingestion, runs object detection on incoming frames, stores results in a database, and includes an Airflow DAG for hourly aggregation.

Key components:
- `producer/` — Kafka producer that reads COCO128 images and publishes base64-encoded frames to Kafka.
- `consumer/` — Spark consumer that reads from Kafka, runs YOLOv5 inference, writes detection events to PostgreSQL, and logs metrics to MLflow.
- `sql/schema.sql` — PostgreSQL schema for dimension/fact tables.
- `airflow/` — Airflow project with a DAG for aggregation and data quality checks.
- `docker-compose.yml` — local infrastructure for Kafka, Zookeeper, PostgreSQL, and MLflow.
- `k8s/` — Kubernetes manifests for the consumer deployment and service.
- `deploy-vision-consumer.ps1` — helper script to rebuild the consumer image and deploy it to Minikube.

## Current status

### Completed / working
- Docker Compose infrastructure defined in `docker-compose.yml`.
- PostgreSQL schema defined in `sql/schema.sql`.
- Producer code exists at `producer/producer.py` and a matching Dockerfile.
- Consumer code exists at `consumer/consumer.py` and a matching Dockerfile.
- Built the consumer image successfully with `docker build -t vision-consumer:latest .\consumer`.
- MLflow service configured in Docker Compose and present in the repository.
- Airflow project exists under `airflow/` and contains the DAG at `airflow/dags/detection_pipline.py`.

### Not finished yet
- Kubernetes/minikube deployment has not been successfully completed.
- Local Minikube connectivity to host services (`Kafka`, `PostgreSQL`, `MLflow`) remains unresolved.
- Cloud integration model registry (AWS S3 / GCP GCS) is not yet completed.
- End-to-end validated deployment of the consumer in Kubernetes is still pending.

## What is available in this repo

- `docker-compose.yml` — starts Zookeeper, Kafka, PostgreSQL, and MLflow locally.
- `.env` — environment variable configuration for database, Kafka, and MLflow.
- `producer/Dockerfile` and `producer/producer.py`.
- `consumer/Dockerfile`, `consumer/consumer.py`, and `consumer/yolov5n.pt`.
- `sql/schema.sql` — star schema with `dim_class`, `dim_time`, `dim_source`, `detection_events`, and `detection_summary`.
- `airflow/` — Airflow project scaffold with DAGs, `.astro` config, and tests.
- `k8s/consumer-deployment.yaml` and `k8s/consumer-service.yaml`.
- `deploy-vision-consumer.ps1` — PowerShell deployment helper for Minikube.

## Recommended local setup steps

1. Install prerequisites:
   - Docker Desktop on Windows with WSL 2 support.
   - Java 17 or newer for PySpark.
   - Python 3.11.
   - Astro CLI for Airflow if you want to run the Airflow project.
   - Minikube + kubectl if you want to continue the Kubernetes work.

2. Populate `.env` with your local values. Example:
   ```ini
   POSTGRES_USER=admin
   POSTGRES_PASSWORD=password123
   POSTGRES_DB=detections_db
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   KAFKA_TOPIC=raw-frames
   MLFLOW_TRACKING_URI=http://localhost:5001
   ```

3. Start local infrastructure:
   ```powershell
   docker compose up -d
   docker compose ps
   ```
   Expected output:
   - `docker compose up -d` should complete without errors.
   - `docker compose ps` should show services for `zookeeper`, `kafka`, `postgres`, and `mlflow` with `State` of `running`.

4. Verify PostgreSQL has the schema loaded:
   ```powershell
   docker exec -it vision-pipeline-postgres psql -U <postgres-user> -d detections_db -c "\dt"
   ```
   Expected output:
   - A table list containing `dim_class`, `dim_time`, `dim_source`, `detection_events`, and `detection_summary`.

5. Run the producer locally:
   ```powershell
   python producer\producer.py
   ```
   Expected output:
   - Lines like `[Producer] Found 128 images. Publishing to topic 'raw-frames'...`
   - Many `Sent frame X/128: <filename>` messages.

6. Run the consumer locally:
   ```powershell
   python consumer\consumer.py
   ```
   Expected output:
   - A startup log such as `[Consumer] Loading YOLOv5n model...` followed by `[Consumer] Streaming started. Waiting for frames...`.
   - Batch processing logs like `[Consumer] Batch 0 | frame=<id> | detections=<n> | latency=<ms>ms`.

7. Verify data in PostgreSQL after the producer and consumer are both running:
   ```powershell
   docker exec -it vision-pipeline-postgres-1 psql -U <postgres-user> -d detections_db
   ```
   Expected output:
   - A `psql` prompt such as `detections_db=>`.

   Then run:
   ```sql
   SELECT class_name, COUNT(*) AS total, ROUND(AVG(confidence)::numeric, 3) AS avg_conf
   FROM detection_events de
   JOIN dim_class dc ON de.class_id = dc.class_id
   GROUP BY class_name
   ORDER BY total DESC;

   SELECT * FROM detection_events LIMIT 5;
   ```
   Expected output:
   - The first query should return rows for one or more classes with counts and average confidence values.
   - The second query should return up to 5 detection rows showing `detection_id`, `frame_id`, `detected_at`, `class_id`, `source_id`, `confidence`, and bounding box columns.

8. Start Airflow if desired:
   ```powershell
   cd airflow
   astro dev start
   ```
   Expected output:
   - The Astro CLI should start the Airflow containers and finish with a message indicating the Airflow webserver is available.
   - You should be able to open `http://localhost:8080` in a browser.

## Kubernetes / Minikube notes

This repo includes Kubernetes manifests and a deployment helper, but the Kubernetes flow is not fully validated.

- `k8s/consumer-deployment.yaml` and `k8s/consumer-service.yaml` exist.
- The deployment manifest references `host.minikube.internal` to reach host services from inside Minikube.
- `deploy-vision-consumer.ps1` is intended to rebuild the image, load it into Minikube, and apply the manifests.

### Known gaps
- The image and manifest are present, but the cluster deployment has not been confirmed working.
- There may be networking issues when Minikube tries to connect to Kafka/Postgres/MLflow running on the host.

## Cloud integration notes

Cloud model registry support was planned but not completed.

- The guide drafted S3/GCS upload scripts under `mlflow/`.
- There is no tested end-to-end model upload or MLflow model registration to cloud storage yet.

## Useful commands (for containerization, but will not be successful because of the previously mentioned)

- Build the consumer image:
  ```powershell
  docker build -t vision-consumer:latest .\consumer
  ```
- Show Docker Compose services:
  ```powershell
  docker compose ps
  ```
- Load consumer image into Minikube:
  ```powershell
  minikube image load vision-consumer:latest
  ```
- Apply Kubernetes manifests:
  ```powershell
  kubectl apply -f k8s\consumer-deployment.yaml
  kubectl apply -f k8s\consumer-service.yaml
  ```
- Re-deploy via helper script:
  ```powershell
  .\deploy-vision-consumer.ps1
  ```
- If PowerShell blocks script execution, allow it for the current session:
  ```powershell
  Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process
  ```
  Expected output:
  - The command should complete without errors.
  - You can then run scripts like `.uild.ps1` or `.
un.ps1` from the current PowerShell session.

## What you can do next

1. Validate the local Docker Compose stack end-to-end with the producer + consumer.
2. Verify and run the Airflow DAG from `airflow/dags/detection_pipline.py`.
3. Troubleshoot Minikube networking to get `k8s/` deployment working.
4. Add cloud model registry support and connect it to MLflow.

## Notes

This README is intended to reflect the current repository state and the exact checkpoint reached: successful setup through local Docker and consumer image build, with Kubernetes/minikube and cloud integration still pending.
