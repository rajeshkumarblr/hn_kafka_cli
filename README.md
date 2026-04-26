# Hacker News & IoT Stress Test Kafka Pipeline

A robust, multi-component Kafka system demonstration featuring real-time data ingestion from Hacker News and a high-volume IoT sensor stress testing suite.

## Project Overview

This repository contains two primary modules:
1.  **Hacker News Pipeline**: Ingests live updates from the HN Firebase API and streams them through Kafka.
2.  **IoT Stress Test**: A high-volume synthetic data platform designed to test Kafka throughput, database persistence, and data integrity across multiple JVM processes.

---

## 🚀 IoT Sensor Stress Test

Located in the `stress-test/` directory, this module simulates a large-scale IoT environment.

### Features
-   **Multi-Process Architecture**: Decoupled Producer, DB-Consumer, and Verifier running in independent JVMs for realistic performance isolation.
-   **Synthetic Data Generation**: High-throughput producer generating temperature and humidity readings with configurable payload overhead (padding) for GC analysis.
-   **Database Persistence**: Optimized PostgreSQL consumer using batch inserts and transaction management.
-   **Automated Verification**: A dedicated integrity auditor that compares Kafka topics against PostgreSQL records to ensure zero data loss.
-   **Visual Monitoring**: Integration with **Kafka-UI** (Docker-based) for deep inspection of topics, offsets, and consumer group health.

### Usage (Stress Test)
```bash
cd stress-test

# 1. Start Kafka and setup partitions
make kafka-setup

# 2. Start the web-based Kafka-UI
make ui-start  # Available at http://localhost:8080

# 3. Run the complete stress test environment
make run

# 4. Monitor logs
tail -f producer.log
tail -f consumer-db.log

# 5. Stop all components
make stop

# 6. Verify data integrity
make verify
```

---

## 📡 Hacker News Pipeline

The core module centered around live data ingestion.

### Design
-   **Infrastructure**: Uses **Kafka 4.2.0** in **KRaft mode** (Zookeeper-less).
-   **HnDataProducer**: Connects to HN SSE API and bridges data to `hn_stories` and `hn_comments` topics.
-   **HnLatencyVerifier**: Performs post-run analysis to calculate end-to-end latency metrics (Min/Max/Avg).

### Usage (HN Pipeline)
```bash
# 1. Start Kafka
make kafka-start

# 2. Run the 10-minute automated test
make run-pipeline

# 3. Stop Kafka
make kafka-stop
```

---

## 🛠️ Prerequisites
-   **Java 17+** (The project is configured for Java 17 LTS)
-   **Maven 3.8+**
-   **Docker** (Required for the Kafka-UI dashboard)
-   **PostgreSQL** (Running on localhost:5432 for the stress test module)
-   **Kafka 4.2.0** (Configured in `manage_kafka.sh`)

## 📈 Learning Goals
-   **Distributed Buffering**: See how Kafka acts as a load balancer/buffer between high-speed producers and slower DB writers.
-   **Backpressure Observation**: Monitor consumer lag grow and shrink during load spikes.
-   **JVM Tuning**: Experiment with `-Xmx` and different GC algorithms (e.g., Parallel vs G1) under the stress test load and observe impacts on throughput.
