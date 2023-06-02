package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;

@Configuration
public class KafkaInitialConfiguration {

    // 创建一个名为test_topic的Topic并设置分区数为8，分区副本数为1
    @Bean
    public NewTopic initialTopic() {
        return new NewTopic("test_topic", 8, (short) 1);
    }

    // 异常处理器
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareListenerErrorHandler() {
        return new ConsumerAwareListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException e, Consumer<?, ?> consumer) {
                System.out.println("消费异常:" + message.getPayload());
                return null;
            }
        };
    }
}
