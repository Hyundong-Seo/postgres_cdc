package net.bitnine.graphizer.controller.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import net.bitnine.graphizer.service.KafkaProducer;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
    private final KafkaProducer producer;

    @Autowired
    KafkaController(KafkaProducer producer){
        System.out.println("===== KafkaController =====");
        this.producer = producer;
    }

    @PostMapping
    @ResponseBody
    public String sendMessage(@RequestParam String message) {
        System.out.println("===== controller sendMessage =====");
        System.out.println("message : " + message);
        
        this.producer.sendMessage(message);

        return "success";
    }
}