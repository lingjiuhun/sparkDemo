package com.cz.example.sparkDemo.config.rabbitmq;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @Author: caozhen
 * @Description: RabbitMq 数据接收
 * @date:
 */
@Component
@RabbitListener(queues = "${rabbitmq.queue.name}")
public class RabbitMqReceiver {
    //数据队列，项目开发中使用redis队列缓存
    public static Queue<String> queue = new LinkedList<String>();

    @RabbitHandler
    public void process(String jsonStr) {
        try {
//            log.info("接收参数 : " + jsonStr);
            //接收数据存入数据队列
            queue.offer(jsonStr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
