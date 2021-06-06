package com.zeed.test.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class TestController {

    @Autowired
    private Producer producer;

    private ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping
    public void testMessage() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("name", "Saheed Yusuf");
        producer.sendMessageToBroker(objectMapper.writeValueAsString(map));
    }

    @PostMapping
    public void test(@RequestBody MyMessage message) {
        producer.sendMessageToBrokerWithMessage(message);
    }
}
