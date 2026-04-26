package com.stress;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SensorConsumer implements Runnable {
    private static final String TOPIC = "iot_sensors";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres"; // Assuming default db
    private static final String USER = "postgres";
    private static final String PASS = ""; // Connect as postgres with no password

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ObjectMapper mapper = new ObjectMapper();

    public void stop() {
        running.set(false);
    }

    public static void main(String[] args) {
        System.out.println("--- SENSOR DB-CONSUMER PROCESS START ---");
        if (args.length > 0 && args[0].equals("--setup")) {
            try {
                setupDatabase();
                System.out.println("Database setup complete. Exiting...");
                System.exit(0);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        new SensorConsumer().run();
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stress-test-db-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Performance tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            
            conn.setAutoCommit(false);
            consumer.subscribe(Collections.singletonList(TOPIC));
            
            String sql = "INSERT INTO sensor_readings (sensor_id, ts, temperature, humidity, location) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);

            System.out.println("Consumer: Starting database persistence (Temp/Humidity)...");
            long count = 0;
            
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode node = mapper.readTree(record.value());
                        pstmt.setString(1, node.get("sensor_id").asText());
                        pstmt.setLong(2, node.get("ts").asLong());
                        pstmt.setDouble(3, node.get("temp").asDouble());
                        pstmt.setDouble(4, node.get("humidity").asDouble());
                        pstmt.setString(5, node.get("location").asText());
                        pstmt.addBatch();
                        count++;
                    } catch (Exception parseEx) {
                        System.err.println("Failed to parse record: " + record.value());
                    }
                }
                
                if (!records.isEmpty()) {
                    pstmt.executeBatch();
                    conn.commit();
                    if (count % 5000 == 0) {
                        System.out.println("Consumer: Persisted " + count + " records to PG.");
                    }
                }
            }
            pstmt.executeBatch();
            conn.commit();
            System.out.println("Consumer: Finished. Total persisted: " + count);

        } catch (Exception e) {
            System.err.println("CRITICAL CONSUMER ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); 
        }
    }

    public static void setupDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             var stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            System.out.println("Database: Initializing table...");
            stmt.execute("DROP TABLE IF EXISTS sensor_readings CASCADE");
            stmt.execute("CREATE TABLE sensor_readings (" +
                    "id SERIAL PRIMARY KEY, " +
                    "sensor_id VARCHAR(50), " +
                    "ts BIGINT, " +
                    "temperature DOUBLE PRECISION, " +
                    "humidity DOUBLE PRECISION, " +
                    "location VARCHAR(100))");
            conn.commit();
            System.out.println("Database: Table sensor_readings initialized with Temp/Humidity columns.");
        }
    }
}
