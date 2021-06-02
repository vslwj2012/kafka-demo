package com.example.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/test")
    public void sendMessage(String message){
        kafkaTemplate.send("topic1",message);
    }
}
