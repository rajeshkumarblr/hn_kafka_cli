# Kafka Pipeline for Hacker News

JVM_OPTS = -Xms1G -Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=100

.PHONY: all build run-producer run-consumer clean kafka-start kafka-stop kafka-status run-pipeline

# Default to building the project
all: build

# Build both the producer and consumer fat JARs using Maven
build:
	mvn clean package

# Run the real-time Hacker News Kafka Producer
run-producer:
	java $(JVM_OPTS) -jar target/producer.jar

# Run the dual-topic Kafka Consumer
run-consumer:
	java $(JVM_OPTS) -jar target/consumer.jar

# Kafka Management
kafka-start:
	./manage_kafka.sh start

kafka-stop:
	./manage_kafka.sh stop

kafka-status:
	./manage_kafka.sh status

# Run the unified 10-minute pipeline
run-pipeline: build
	./manage_kafka.sh start
	java $(JVM_OPTS) -jar target/pipeline.jar
	./manage_kafka.sh stop

# Clean up build artifacts and log files
clean:
	mvn clean
	rm -f *.log
