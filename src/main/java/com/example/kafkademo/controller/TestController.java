package com.example.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/test")
    public void sendMessage(String message) {
        kafkaTemplate.send("topic9", message);
    }

    /**
     * kafkaTemplate提供了一个回调方法addCallback，我们可以在回调方法中监控消息是否发送成功 或 失败时做补偿处理，有两种写法，
     */
    @GetMapping("/callback")
    public void sendMessage2(String message) {
        //第一种写法
        kafkaTemplate.send("test_topic", message).addCallback(
                success -> {
                    //消息发送到的topic
                    String topic = success.getRecordMetadata().topic();
                    //消息发送到的分区
                    int partition = success.getRecordMetadata().partition();
                    //消息在分区内的offset
                    long offset = success.getRecordMetadata().offset();
                    System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
                }, failure -> {
                    System.out.println("发送消息失败:" + failure.getMessage());
                }
        );

        //第二种写法
        kafkaTemplate.send("topic1", message).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("发送消息失败:" + throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("发送消息成功：" + result.getRecordMetadata().topic() + "-"
                        + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
            }
        });
    }

    @GetMapping("/transaction")
    public void sendMessage3(String message) {
//        kafkaTemplate.executeInTransaction(kafkaOperations -> {
//            //声明事物，异常后回滚
//            kafkaOperations.send("topic1", "事物声明:" + message);
//            throw new RuntimeException("fail");
//        });

        //不声明事物，异常后已经发送成功
        kafkaTemplate.send("topic1", "事物未声明:" + message);
        throw new RuntimeException("fail");
    }
}
