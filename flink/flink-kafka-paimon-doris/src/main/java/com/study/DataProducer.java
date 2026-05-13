package com.study;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Creates a new Kafka topic and continuously sends random order events.
 * Run this first to populate the topic before starting KafkaToPaimon.
 *
 * Usage: mvn exec:java -Dexec.mainClass="com.study.DataProducer"
 */
public class DataProducer {

    private static final String BOOTSTRAP_SERVERS = "172.31.249.211:9092";
    private static final String TOPIC = "paimon_order_events";
    private static final int PARTITIONS = 3;
    private static final short REPLICATION = 1;

    private static final String[] PRODUCTS = {
            "iPhone 15", "MacBook Pro", "iPad Air", "AirPods Pro",
            "Apple Watch", "Mac Mini", "Magic Keyboard", "Studio Display"
    };

    private static final Random RANDOM = new Random();
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        // Create topic if not exists
        createTopic();

        // Start producing
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");

        int orderId = 1;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing to topic: " + TOPIC);
            while (true) {
                String json = buildOrderJson(orderId);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(orderId), json),
                        (metadata, ex) -> {
                            if (ex == null) {
                                System.out.println("Sent: offset=" + metadata.offset() + " | " + json);
                            } else {
                                System.err.println("Send failed: " + ex.getMessage());
                            }
                        });
                orderId++;
                Thread.sleep(2000); // 2 seconds between messages
            }
        }
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        try (AdminClient admin = AdminClient.create(props)) {
            boolean exists = admin.listTopics().names().get().contains(TOPIC);
            if (!exists) {
                admin.createTopics(Collections.singletonList(
                        new NewTopic(TOPIC, PARTITIONS, REPLICATION))).all().get();
                System.out.println("Created topic: " + TOPIC);
            } else {
                System.out.println("Topic already exists: " + TOPIC);
            }
        }
    }

    private static String buildOrderJson(int orderId) {
        String now = LocalDateTime.now().format(FMT);
        String product = PRODUCTS[RANDOM.nextInt(PRODUCTS.length)];
        int userId = 1000 + RANDOM.nextInt(9000);
        double price = 999 + RANDOM.nextInt(15000);
        int quantity = 1 + RANDOM.nextInt(3);

        return String.format(
                "{\"order_id\":%d,\"user_id\":%d,\"product_name\":\"%s\",\"price\":%.2f,\"quantity\":%d,\"create_time\":\"%s\",\"update_time\":\"%s\"}",
                orderId, userId, product, price, quantity, now, now);
    }
}
