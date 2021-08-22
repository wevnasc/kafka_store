package store.producer;

import store.kafka.KafkaDispatcher;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var order = "123,5312,1234";
                dispatcher.send("STORE_NEW_ORDER", key, order);

                var email = "Thank you for you order, we are processing your order!";
                dispatcher.send("STORE_SEND_EMAIL", key, email);
            }
        }
    }
}
