package com.cz.example.sparkDemo.config.rabbitmq;

import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: caozhen
 * @Description: mq 队列配置
 * @date: 2018/8/15 20:05
 */
@Configuration
public class RabbitConfig {

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    /**
     * @return
     * @Description: 绑定指定队列
     */
    @Bean
    public Queue queue() {
        return new Queue(queueName);
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
