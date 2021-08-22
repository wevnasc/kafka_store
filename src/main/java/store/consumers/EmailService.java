package store.consumers;

import store.kafka.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var fraudService = new EmailService();
        try(var server = new KafkaServer("STORE_SEND_EMAIL", EmailService.class.getSimpleName(), fraudService::parse)){
            server.run();
        }
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------------");
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Email sent");
    }
}
