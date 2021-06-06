package com.zeed.test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, MyMessage> messageKafkaTemplate;

    private static final String TOPIC = "test";
    private static final String TOPIC2 = "test2";
    private static final String STREAM_TOPIC = "stream-topic";

    public void sendMessageToBroker(String message) {
        Properties properties = new Properties();
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "localhost:9092");
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<>(STREAM_TOPIC,message));
    }

    public void sendMessageToBrokerWithMessage(MyMessage message) {
        messageKafkaTemplate.send(TOPIC2, message);
    }
}
