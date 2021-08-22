package store.consumers;

import store.kafka.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try(var server = new KafkaServer("STORE_NEW_ORDER", FraudDetectorService.class.getSimpleName(), fraudService::parse)){
            server.run();
        }
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------------");
        System.out.println("processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Order processed");
    }
}
