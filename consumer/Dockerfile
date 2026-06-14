FROM python:3.11-bookworm

RUN apt-get update && apt-get install -y openjdk-17-jdk wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY yolov5n.pt .

CMD ["python", "consumer.py"]