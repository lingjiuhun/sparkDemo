package com.cz.example.sparkDemo.config.rabbitmq;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * RabbitMq队列数据生产
 * @date: 2018/10/31 15:54
 */
@Component
public class RabbitMqProduct extends Thread  {
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private RabbitConfig rabbitConfig;

    public RabbitMqProduct(){
        super();
    }

    @Override
    public void run() {
        int i=0;
        while(true){
            //发送到mq
//            this.rabbitTemplate.convertAndSend("alarm_data", JSON.toJSONString(item));
            this.rabbitTemplate.convertAndSend(rabbitConfig.getQueueName(), "Mq——message: " + i++);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
