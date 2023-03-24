package net.bitnine.graphizer.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
@Configuration
public class KafkaConsumerConfig {
    private Environment env;
    
    @Autowired
    KafkaConsumerConfig(Environment env) {
        this.env = env;
    }
}