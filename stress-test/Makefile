# Kafka Stress Test Makefile

PIDS = producer.pid consumer-db.pid
JVM_OPTS = -Xmx1G

.PHONY: all build run stop clean kafka-start kafka-stop kafka-setup verify

all: build

build:
	mvn clean package

kafka-start:
	../manage_kafka.sh start

kafka-stop:
	../manage_kafka.sh stop

kafka-setup: kafka-start
	/home/rajesh/kafka/kafka_2.13-4.2.0/bin/kafka-topics.sh --create --topic iot_sensors --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

ui-start:
	@if [ $$(docker ps -q -f name=kafka-ui-stress) ]; then \
		echo "Kafka-UI is already running on http://localhost:8080"; \
	elif [ $$(docker ps -aq -f name=kafka-ui-stress) ]; then \
		echo "Starting existing Kafka-UI-stress container..."; \
		docker start kafka-ui-stress; \
	else \
		echo "Starting new Kafka-UI on http://localhost:8080..."; \
		docker run -d --name kafka-ui-stress \
			-p 8080:8080 \
			--network="host" \
			-e KAFKA_CLUSTERS_0_NAME=local \
			-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
			provectuslabs/kafka-ui:latest; \
	fi

ui-stop:
	@echo "Stopping Kafka-UI..."
	@docker stop kafka-ui-stress > /dev/null 2>&1 || true
	@docker rm kafka-ui-stress > /dev/null 2>&1 || true

run: build
	@echo "Ensuring Kafka is started..."
	../manage_kafka.sh start
	@echo "Initializing Database..."
	java -jar target/stress-consumer-db.jar --setup
	@echo "Starting Stress Test components..."
	java $(JVM_OPTS) -jar target/stress-producer.jar > producer.log 2>&1 & echo $$! > producer.pid
	java $(JVM_OPTS) -jar target/stress-consumer-db.jar > consumer-db.log 2>&1 & echo $$! > consumer-db.pid
	@echo "------------------------------------------------"
	@echo "PRODUCER   : Background (log: producer.log)"
	@echo "DB-WRITER  : Background (log: consumer-db.log)"
	@echo "------------------------------------------------"
	@echo "Run 'make stop' to terminate all components."

stop:
	@for pid_file in $(PIDS); do \
		if [ -f $$pid_file ]; then \
			pid=$$(cat $$pid_file); \
			echo "Stopping process $$pid..."; \
			kill $$pid 2>/dev/null || true; \
			rm $$pid_file; \
		fi; \
	done
	@$(MAKE) ui-stop
	@echo "All components stopped."

verify:
	@echo "Running Data Integrity Verification..."
	java -jar target/stress-verifier.jar

clean:
	mvn clean
	rm -f *.log *.pid
