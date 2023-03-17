package net.bitnine.graphizer.service;

import java.io.IOException;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    @KafkaListener(topics = "fulfillment.public.sample", groupId = "3")
    public void consume(String message) throws IOException {
        System.out.println("===== consumer sendMessage =====");
        System.out.println(message);
    }
}