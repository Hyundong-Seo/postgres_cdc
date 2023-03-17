package net.bitnine.graphizer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class KafkaProducer {
    private static final String TOPIC = "fulfillment.public.sample";

    @Autowired
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        System.out.println("===== producer sendMessage =====");
        System.out.println("message : " + message);
        this.kafkaTemplate.send(TOPIC, message);
    }
}