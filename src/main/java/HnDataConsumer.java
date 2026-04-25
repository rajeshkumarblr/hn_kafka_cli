import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class HnDataConsumer implements Runnable {

    private static final String CONSUMER_LOG = "consumer.log";
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private PrintWriter logWriter;

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        initializeLog();
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hn-pipeline-verification-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("hn_stories", "hn_comments"));
            System.out.println("Consumer: Started and subscribed.");

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            if (running.get()) e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    private void initializeLog() {
        try {
            logWriter = new PrintWriter(new BufferedWriter(new FileWriter(CONSUMER_LOG, false)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize consumer log", e);
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            JsonNode itemNode = mapper.readTree(record.value());
            long id = itemNode.path("id").asLong();
            String type = itemNode.path("type").asText().toUpperCase();
            long timestamp = System.currentTimeMillis();

            logWriter.printf("%d,%s,%d\n", timestamp, type, id);
            logWriter.flush();
        } catch (Exception e) {
            System.err.println("Consumer: Error parsing record: " + e.getMessage());
        }
    }

    private void cleanup() {
        System.out.println("Consumer: Shutting down...");
        if (logWriter != null) logWriter.close();
    }

    public static void main(String[] args) {
        new HnDataConsumer().run();
    }
}
