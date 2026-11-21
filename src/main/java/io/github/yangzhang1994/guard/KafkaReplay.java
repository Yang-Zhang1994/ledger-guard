package io.github.yangzhang1994.guard;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes a file of event lines to Kafka, keyed by SKU like StockLedger's outbox does, so each
 * SKU's events stay in one partition.
 *
 * <pre>KafkaReplay --kafka localhost:9092 --topic inventory-events --file data/events.jsonl</pre>
 */
public final class KafkaReplay {

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Job.parse(args);
        long n = replay(o.getOrDefault("kafka", "localhost:9092"), o.getOrDefault("topic", "inventory-events"),
                Path.of(o.getOrDefault("file", "data/events.jsonl")));
        System.out.printf("published %,d lines%n", n);
    }

    public static long replay(String servers, String topic, Path file) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        long n = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p);
             BufferedReader in = Files.newBufferedReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String key;
                try {
                    key = InventoryEvent.parse(line).sku;
                } catch (Exception e) {
                    key = "garbage"; // still sent: the job must cope with it
                }
                producer.send(new ProducerRecord<>(topic, key, line));
                n++;
            }
            producer.flush();
        }
        return n;
    }
}
