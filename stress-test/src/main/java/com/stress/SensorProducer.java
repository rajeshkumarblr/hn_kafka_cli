package com.stress;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class SensorProducer implements Runnable {
    private static final String TOPIC = "iot_sensors";
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();
    


    public void stop() {
        running.set(false);
    }

    public static void main(String[] args) {
        System.out.println("--- SENSOR PRODUCER PROCESS START ---");
        new SensorProducer().run();
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance tuning
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072); // 128KB batch
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer: Starting synthetic data generation (Temp/Humidity)...");
            long count = 0;
            String padding = "x".repeat(1024); // 1KB padding is enough for general stress
            while (running.get()) {
                String sensorId = "sensor-" + random.nextInt(10); // 10 unique sensors
                double temp = 20.0 + (random.nextDouble() * 15.0); // 20-35 C
                double humidity = 30.0 + (random.nextDouble() * 40.0); // 30-70 %
                long ts = System.currentTimeMillis();
                
                String payload = String.format("{\"sensor_id\":\"%s\",\"ts\":%d,\"temp\":%.2f,\"humidity\":%.2f,\"location\":\"Zone-%d\",\"padding\":\"%s\"}",
                        sensorId, ts, temp, humidity, random.nextInt(5), padding);

                producer.send(new ProducerRecord<>(TOPIC, sensorId, payload));
                count++;
                
                if (count % 5000 == 0) {
                    System.out.println("Producer: Sent " + count + " records.");
                }
                
                // Slight sleep to keep it manageable but still high volume
                try { Thread.sleep(2); } catch (InterruptedException e) { break; }
            }
            System.out.println("Producer: Finished. Total sent: " + count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
