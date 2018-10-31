package com.cz.example.sparkDemo.config.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

/**
 *
 * 启动kafka数据产生
 * @author
 *
 */
@Component
public class KafkaProduct extends Thread {


    @Value("${kafka.broker.list}")
    private String metadatabrokerlist;
    @Value("${spark.kafka.topics}")
    private String topic;

    public KafkaProduct(){
        super();
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        int i=0;
        while(true){
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "localhost:9092");// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }
}
