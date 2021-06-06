package com.zeed.test.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    private Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "test", groupId = "group_id_1")
    public void consumeMessage(String message) {
        logger.info(message);
    }

    @KafkaListener(topics = "test2", groupId = "group_id_1")
    public void consumeMessage(MyMessage message) {
        logger.info("[{}]",message);
    }

}
