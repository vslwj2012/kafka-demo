package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaConsumer {

    @Autowired
    private ConsumerFactory consumerFactory;

    @KafkaListener(id = "consumer1", topics = {"topic1", "test_topic", "topic8"}, groupId = "test-consumer-group")
    public void onMessage(ConsumerRecord<?, ?> record) {
        System.out.println("简单消费:" + record.topic() + "-"
                + record.partition() + "-" + record.value());
    }

    /**
     * 同时监听topic1和test_topic，监听topic1的0号分区、test_topic的 "0号" 分区，
     * ① id：消费者ID；
     * <p>
     * ② groupId：消费组ID；
     * <p>
     * ③ topics：监听的topic，可监听多个；
     * <p>
     * ④ topicPartitions：可配置更加详细的监听信息，可指定topic、parition、offset监听。
     */
//    @KafkaListener(id = "consumer2",groupId = "test-consumer-group",
//    topicPartitions = {
//            @TopicPartition(topic = "topic1",partitions = {"0"}),
//            @TopicPartition(topic = "test_topic",partitions = "0")
//    })
//    public void onMessage2(ConsumerRecord<?,?> record){
//        System.out.println("topic:"+record.topic()+"|partition:"+record.partition()+"|offset:"+record.offset()+"|value:"+record.value());
//    }
    @KafkaListener(id = "consumer3", groupId = "test_consumer_group", topics = "topic4")
    public void onMessage3(List<ConsumerRecord<?, ?>> records) {
        System.out.println("批量消费一次,records.size()=" + records.size());
        for (ConsumerRecord<?, ?> record : records) {
            System.out.println(record.value());
        }
    }

    @KafkaListener(id = "consumer4", groupId = "test_consumer_group", topics = "topic5", errorHandler = "consumerAwareListenerErrorHandler")
    public void onMessage4(ConsumerRecord<?, ?> record) throws Exception {
        System.out.println("简单消费异常模拟:" + record.value());
        throw new Exception("简单消费异常模拟");
    }

    //消息过滤器
    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        //被过滤的消息将被丢弃
        factory.setAckDiscarded(true);
        //消息过滤策略
        factory.setRecordFilterStrategy(consumerRecord -> {
            if (Integer.parseInt(consumerRecord.value().toString()) % 2 == 0) {
                return false;
            }
            //返回true则被过滤
            return true;
        });
        return factory;
    }

    @KafkaListener(id = "consumer5", groupId = "test_consumer_group", topics = "topic6", containerFactory = "filterContainerFactory")
    public void onMessage5(ConsumerRecord<?, ?> record) {
        System.out.println("过滤消息测试:" + record.value());
    }

    /**
     * 消息转发
     *
     * @param record
     */
//    @KafkaListener(id = "consumer6", groupId = "test_consumer_group", topics = "topic7")
//    @SendTo("topic8")
//    public String onMessage6(ConsumerRecord<?, ?> record) {
//        return record.value()+"-forward message";
//    }


}