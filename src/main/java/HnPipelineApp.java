public class HnPipelineApp {

    public static void main(String[] args) {
        System.out.println("Starting Unified HN Pipeline Application...");

        HnDataProducer producerTask = new HnDataProducer();
        HnDataConsumer consumerTask = new HnDataConsumer();

        Thread producerThread = new Thread(producerTask, "ProducerThread");
        Thread consumerThread = new Thread(consumerTask, "ConsumerThread");

        consumerThread.start();
        // Brief delay to ensure consumer group is ready
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        
        producerThread.start();

        System.out.println("Pipeline is running. Waiting for 10 minutes...");
        
        try {
            // Wait for 10 minutes
            Thread.sleep(10 * 60 * 1000);
        } catch (InterruptedException e) {
            System.err.println("Pipeline interrupted.");
        }

        System.out.println("--- 10 Minutes Elapsed. Stopping Producer... ---");
        producerTask.stop();
        
        try {
            producerThread.join(30000); // Wait for producer to stop
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Waiting for consumer to drain remaining messages (30s)...");
        try {
            Thread.sleep(30000);
        } catch (InterruptedException ignored) {}

        System.out.println("Stopping Consumer...");
        consumerTask.stop();

        try {
            consumerThread.join(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Run complete. Starting verification...");
        HnLatencyVerifier.verify("producer.log", "consumer.log");
        
        System.out.println("Unified App Exit.");
    }
}
