import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HnDataProducer implements Runnable {

    private static final String UPDATES_URL = "https://hacker-news.firebaseio.com/v0/updates.json";
    private static final String ITEM_URL_FORMAT = "https://hacker-news.firebaseio.com/v0/item/%d.json";
    private static final String PRODUCER_LOG = "producer.log";
    
    private final ObjectMapper mapper = new ObjectMapper();
    private KafkaProducer<String, String> kafkaProducer;
    private final ExecutorService fetcherExecutor = Executors.newFixedThreadPool(10);
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .executor(fetcherExecutor)
            .build();
    
    private final AtomicBoolean running = new AtomicBoolean(true);
    private PrintWriter logWriter;

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        initializeLog();
        initializeKafka();

        System.out.println("Producer: Starting Hacker News updates listener...");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(UPDATES_URL))
                .header("Accept", "text/event-stream")
                .GET()
                .build();

        try {
            // We use a simpler streaming approach to allow interruption
            client.sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                    .thenAccept(response -> {
                        response.body().takeWhile(line -> running.get()).forEach(line -> {
                            if (line.startsWith("data: ")) {
                                String jsonData = line.substring(6);
                                if (!jsonData.trim().equals("null")) {
                                    handleUpdatesPayload(jsonData);
                                }
                            }
                        });
                    })
                    .join();

        } catch (Exception e) {
            if (running.get()) {
                e.printStackTrace();
            }
        } finally {
            cleanup();
        }
    }

    private void initializeLog() {
        try {
            logWriter = new PrintWriter(new BufferedWriter(new FileWriter(PRODUCER_LOG, false)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize producer log", e);
        }
    }

    private void initializeKafka() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        kafkaProducer = new KafkaProducer<>(props);
    }

    private void handleUpdatesPayload(String jsonData) {
        try {
            JsonNode root = mapper.readTree(jsonData);
            JsonNode itemsNode = root.at("/data/items");
            if (itemsNode.isArray()) {
                for (JsonNode idNode : itemsNode) {
                    if (!running.get()) break;
                    fetchAndPublishItemAsync(idNode.asLong());
                }
            }
        } catch (Exception e) {
            System.err.println("Producer: Failed to parse updates: " + e.getMessage());
        }
    }

    private void fetchAndPublishItemAsync(long id) {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(String.format(ITEM_URL_FORMAT, id)))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenAccept(res -> {
                    if (res.statusCode() != 200 || !running.get()) return;
                    processItem(id, res.body());
                });
    }

    private synchronized void processItem(long id, String itemJson) {
        try {
            JsonNode itemNode = mapper.readTree(itemJson);
            String type = itemNode.path("type").asText();
            long timestamp = System.currentTimeMillis();

            if ("story".equals(type)) {
                kafkaProducer.send(new ProducerRecord<>("hn_stories", String.valueOf(id), itemJson));
                logWriter.printf("%d,STORY,%d\n", timestamp, id);
                logWriter.flush();
            } else if ("comment".equals(type)) {
                String parentId = itemNode.path("parent").asText();
                kafkaProducer.send(new ProducerRecord<>("hn_comments", parentId, itemJson));
                logWriter.printf("%d,COMMENT,%d\n", timestamp, id);
                logWriter.flush();
            }
        } catch (Exception e) {
            System.err.println("Producer: Error processing item " + id + ": " + e.getMessage());
        }
    }

    private void cleanup() {
        System.out.println("Producer: Shutting down...");
        if (kafkaProducer != null) kafkaProducer.close();
        if (logWriter != null) logWriter.close();
        fetcherExecutor.shutdown();
    }

    public static void main(String[] args) {
        new HnDataProducer().run();
    }
}
