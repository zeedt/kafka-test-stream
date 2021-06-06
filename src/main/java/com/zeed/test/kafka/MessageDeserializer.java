package com.zeed.test.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;


public class MessageDeserializer implements Deserializer<MyMessage> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public MyMessage deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, MyMessage.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
