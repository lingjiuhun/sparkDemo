package com.cz.example.sparkDemo.config.rabbitmq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * spring boot 容器加载完成后执行
 * 启动kafka数据接收和处理
 *
 * @author
 */
@Component
@Order(value = 3)
public class SparkRabbitMqStartup implements CommandLineRunner {

    @Autowired
    SparkRabbitMqSteamExecutor sparkRabbitMqSteamExecutor;
    @Autowired
    RabbitMqProduct rabbitMqProduct;

    @Override
    public void run(String... args) throws Exception {
        //启动数据生产者
        new Thread(rabbitMqProduct).start();

        //启动消费者
        Thread thread = new Thread(sparkRabbitMqSteamExecutor);
        thread.start();
    }

}
