# Hacker News Kafka Pipeline (HN CLI)

A real-time, end-to-end data pipeline that ingests live Hacker News updates, processes them through Kafka, and provides automated performance verification.

## Goal
The primary objective of this project is to demonstrate a robust Java-based Kafka pipeline that:
1.  **Ingests** real-time data from the Hacker News Firebase (SSE) API.
2.  **Streams** stories and comments into distinct Kafka topics.
3.  **Persists** data into local logs via a specialized consumer.
4.  **Verifies** the entire flow by measuring end-to-end latency and data integrity over a controlled run duration.

## Design
The system is built with a decoupled architecture focusing on reliability and visibility:

-   **Infrastructure**: Uses **Kafka 4.2.0** in **KRaft mode**, enabling a modern, Zookeeper-less deployment.
-   **Producer (`HnDataProducer`)**: 
    -   Connects to HN updates stream via SSE.
    -   Uses an asynchronous HTTP client to bridge the web stream with Kafka topics (`hn_stories`, `hn_comments`).
    -   Logs every production event with millisecond timestamps for verification.
-   **Consumer (`HnDataConsumer`)**:
    -   Subscribes to all data topics.
    -   Ensures high throughput with optimized poll settings.
    -   Logs arrival events to track latency.
-   **Unified Orchestrator (`HnPipelineApp`)**: 
    -   Manages the startup/shutdown of Kafka.
    -   Runs the producer and consumer in parallel threads for 10 minutes (or any configured duration).
    -   Ensures a graceful "drain" phase where the consumer finishes processing after the producer stops.
-   **Verification (`HnLatencyVerifier`)**: 
    -   Performs post-run analysis by matching IDs between producer and consumer logs.
    -   Calculates metrics: Min, Max, and Average Latency.

## Usage

### Prerequisites
-   Java 11+
-   Maven
-   Kafka installation (configured in `manage_kafka.sh`)

### Commands
All operations are unified via the `Makefile`:

-   **Start Kafka Broker**: `make kafka-start`
-   **Check Status**: `make kafka-status`
-   **Run 10-Minute Pipeline Test**: `make run-pipeline`
-   **Stop Kafka Broker**: `make kafka-stop`
-   **Clean Build Artifacts**: `make clean`

### Verification Output
At the end of a `run-pipeline` execution, you will see a summary like this:
```text
--- Pipeline Verification Report ---
Messages Produced: 448
Messages Consumed: 448
Messages Missing:  0
Latency (ms): MIN: 42 | MAX: 501 | AVG: 120.45
SUCCESS: All produced messages were consumed.
```
