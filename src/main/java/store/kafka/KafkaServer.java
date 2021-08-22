package store.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings("InfiniteLoopStatement")
public class KafkaServer implements Closeable {

    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction listener;

    public KafkaServer(String topic, String group, ConsumerFunction listener) {
        this.topic = topic;
        this.listener = listener;
        this.consumer = new KafkaConsumer<>(properties(group));
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("records found " + records.count());

                for (var record : records) {
                    this.listener.parse(record);
                }
            }
        }
    }

    private Properties properties(String group) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
