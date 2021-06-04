package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 6、定时启动、停止监听器
 * <p>
 * 默认情况下，当消费者项目启动的时候，监听器就开始工作，监听消费发送到指定topic的消息，那如果我们不想让监听器立即工作，想让它在我们指定的时间点开始工作，或者在我们指定的时间点停止工作，该怎么处理呢——使用KafkaListenerEndpointRegistry，下面我们就来实现：
 * <p>
 * ① 禁止监听器自启动；
 * <p>
 * ② 创建两个定时任务，一个用来在指定时间点启动定时器，另一个在指定时间点停止定时器；
 * <p>
 * 新建一个定时任务类，用注解@EnableScheduling声明，KafkaListenerEndpointRegistry 在SpringIO中已经被注册为Bean，直接注入，设置禁止KafkaListener自启动，
 */
@EnableScheduling
@Component
public class CronTimer {
    /**
     * @KafkaListener注解所标注的方法并不会在IOC容器中被注册为Bean， 而是会被注册在KafkaListenerEndpointRegistry中，
     * 而KafkaListenerEndpointRegistry在SpringIOC中已经被注册为Bean
     **/

    @Resource
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private ConsumerFactory consumerFactory;

    // 监听容器工厂（设置禁止kafkaListener自启动）
    @Bean
    public ConcurrentKafkaListenerContainerFactory delayContainerFactory() {
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConsumerFactory(consumerFactory);
        //禁止kafkaListener自启动
        containerFactory.setAutoStartup(false);
        return containerFactory;
    }

    //监听器
    @KafkaListener(id = "timingConsumer", topics = "topic9", containerFactory = "delayContainerFactory")
    public void onMessage1(ConsumerRecord<?, ?> record) {
        System.out.println("定时消费成功:" + record.topic() + "-" + record.partition() + "-" + record.value());
    }

    //定时启动监听器 定时在十点三十五启动
    @Scheduled(cron = "0 40 10 * * ?")
    public void startListener() {
        if (!kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").isRunning()) {
            System.out.println("启动监听器...");
            kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").start();
        }
        System.out.println(kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").isRunning());
    }

    //定时停止监听器 定时在十点三十六关闭
    @Scheduled(cron = "0 41 10 * * ?")
    public void shutDownListener() {
        if (kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").isRunning()) {
            System.out.println("关闭监听器...");
            kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").pause();
        }
        System.out.println(kafkaListenerEndpointRegistry.getListenerContainer("timingConsumer").isRunning());
    }
}
