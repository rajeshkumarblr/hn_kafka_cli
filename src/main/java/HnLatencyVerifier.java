import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class HnLatencyVerifier {

    public static void verify(String producerLog, String consumerLog) {
        Map<Long, Long> produced = new HashMap<>(); // ID -> Timestamp
        
        System.out.println("\n--- Pipeline Verification Report ---");

        try (BufferedReader pr = new BufferedReader(new FileReader(producerLog))) {
            String line;
            while ((line = pr.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    produced.put(Long.parseLong(parts[2]), Long.parseLong(parts[0]));
                }
            }
        } catch (Exception e) {
            System.err.println("Verifier: Error reading producer log: " + e.getMessage());
            return;
        }

        long minLatency = Long.MAX_VALUE;
        long maxLatency = 0;
        long totalLatency = 0;
        int consumedCount = 0;
        int missingCount = 0;

        try (BufferedReader cr = new BufferedReader(new FileReader(consumerLog))) {
            String line;
            while ((line = cr.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    long id = Long.parseLong(parts[2]);
                    long consumeTs = Long.parseLong(parts[0]);
                    
                    if (produced.containsKey(id)) {
                        long produceTs = produced.get(id);
                        long latency = consumeTs - produceTs;
                        
                        minLatency = Math.min(minLatency, latency);
                        maxLatency = Math.max(maxLatency, latency);
                        totalLatency += latency;
                        consumedCount++;
                        produced.remove(id); // Mark as consumed
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Verifier: Error reading consumer log: " + e.getMessage());
            return;
        }

        missingCount = produced.size();

        System.out.printf("Messages Produced: %d\n", consumedCount + missingCount);
        System.out.printf("Messages Consumed: %d\n", consumedCount);
        System.out.printf("Messages Missing:  %d\n", missingCount);
        
        if (consumedCount > 0) {
            System.out.printf("Latency (ms): MIN: %d | MAX: %d | AVG: %.2f\n",
                    minLatency, maxLatency, (double) totalLatency / consumedCount);
        }

        if (missingCount == 0) {
            System.out.println("SUCCESS: All produced messages were consumed.");
        } else {
            System.out.println("WARNING: There are unconsumed messages.");
        }
        System.out.println("------------------------------------\n");
    }
}
