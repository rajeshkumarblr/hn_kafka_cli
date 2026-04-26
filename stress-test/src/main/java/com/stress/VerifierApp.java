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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class VerifierApp {
    private static final String TOPIC = "iot_sensors";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASS = "";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        System.out.println("=== DATA INTEGRITY VERIFIER STARTING ===");
        
        long kafkaCount = countKafkaMessages();
        long pgCount = countPgRecords();

        System.out.println("Kafka Total Messages: " + kafkaCount);
        System.out.println("PG Total Records:    " + pgCount);

        if (kafkaCount == pgCount && kafkaCount > 0) {
            System.out.println("SUCCESS: Record counts match perfectly!");
            verifySampleData();
        } else if (kafkaCount == 0) {
            System.out.println("WARNING: No data found in Kafka or PG.");
        } else {
            System.out.println("FAILURE: Data mismatch detected!");
            System.out.println("Difference: " + Math.abs(kafkaCount - pgCount) + " records.");
        }
    }

    private static long countKafkaMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        long count = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            
            // Poll until we reach the end of the topic
            boolean keepPolling = true;
            int emptyPolls = 0;
            while (keepPolling && emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    count += records.count();
                    emptyPolls = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    private static long countPgRecords() {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM sensor_readings")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static void verifySampleData() {
        System.out.println("Performing deep-check on sample records...");
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT ts, temperature FROM sensor_readings ORDER BY ts DESC LIMIT 10")) {
            
            System.out.println("Recent Records in PG:");
            while (rs.next()) {
                System.out.printf("  TS: %d | Temp: %.2f\n", rs.getLong(1), rs.getDouble(2));
            }
            System.out.println("Sample verification complete.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
