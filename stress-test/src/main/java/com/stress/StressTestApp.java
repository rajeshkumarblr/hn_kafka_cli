package com.stress;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

public class StressTestApp {
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("KAFKA-TO-POSTGRES PIPELINE (CLEAN RUN)");
        System.out.println("========================================");

        try {
            SensorConsumer.setupDatabase();
        } catch (Exception e) {
            System.err.println("CRITICAL: Failed to initialize database. Exiting.");
            e.printStackTrace();
            System.exit(1);
        }

        List<SensorProducer> producers = new ArrayList<>();
        List<SensorConsumer> consumers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        // Start 2 Consumers
        System.out.println("Starting 2 Consumer threads...");
        for (int i = 0; i < 2; i++) {
            SensorConsumer consumer = new SensorConsumer();
            consumers.add(consumer);
            Thread ct = new Thread(consumer, "ConsumerThread-" + i);
            ct.start();
            threads.add(ct);
        }

        // Start 2 Producers
        System.out.println("Starting 2 Producer threads...");
        for (int i = 0; i < 2; i++) {
            SensorProducer producer = new SensorProducer();
            producers.add(producer);
            Thread pt = new Thread(producer, "ProducerThread-" + i);
            pt.start();
            threads.add(pt);
        }

        // Run for 10 minutes
        long runTimeMs = 10 * 60 * 1000;
        System.out.println("Pipeline will run for " + (runTimeMs / 1000) + " seconds.");

        try {
            Thread.sleep(runTimeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Shutting down pipeline gracefully...");
        for (SensorProducer p : producers) p.stop();
        
        for (Thread t : threads) {
            if (t.getName().contains("Producer")) {
                try { t.join(); } catch (InterruptedException e) {}
            }
        }

        try { Thread.sleep(5000); } catch (InterruptedException e) {}
        
        for (SensorConsumer c : consumers) c.stop();
        for (Thread t : threads) {
            if (t.getName().contains("Consumer")) {
                try { t.join(); } catch (InterruptedException e) {}
            }
        }

        System.out.println("Run Complete.");
    }
}
