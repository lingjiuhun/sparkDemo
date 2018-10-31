package com.cz.example.sparkDemo.config.rabbitmq;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @Author: caozhen
 * @Description: 自定义RabbitMq接收streaming类
 * @date: 2018/10/31 16:30
 */
public class SparkRabbitMqSteamExecutorReceiver extends Receiver<String> {

    private static Logger logger = LoggerFactory.getLogger(SparkRabbitMqSteamExecutorReceiver.class);


    /**
     *
     * @author
     * @date
     * @version
     */
    private static final long serialVersionUID = 5817531198342629811L;

    public SparkRabbitMqSteamExecutorReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        new Thread(this::doStart).start();
        logger.info("开始启动Receiver...");
        //doStart();
    }

    public void doStart() {
        while(!isStopped()) {
            //返回队列中第一元素并从队列中移除
            String value = RabbitMqReceiver.queue.poll();
            //存入数据
            if(!Objects.isNull(value)) {
                store(String.valueOf(value));
            }
        }
    }


    @Override
    public void onStop() {
        logger.info("即将停止Receiver...");
    }

}
