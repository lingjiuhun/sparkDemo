package com.cz.example.sparkDemo.config.rabbitmq;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;
/**
 * @Author: caozhen
 * @Description: RabbitMq SparkStream 消费任务执行
 * @date: 2018/10/31 16:31
 */
@Component
public class SparkRabbitMqSteamExecutor implements Serializable,Runnable{

    private static final Logger logger = LoggerFactory.getLogger(SparkRabbitMqSteamExecutor.class);
    @Autowired
    private JavaSparkContext sc;

    @Override
    public void run() {
        try {
            //批间隔时间
            JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(10));
            //指定从自定义streaming流中获取数据
            JavaReceiverInputDStream<String> lines = jsc.receiverStream(new SparkRabbitMqSteamExecutorReceiver(StorageLevel.MEMORY_AND_DISK_2()));
            //打印流中每个数据值
            lines.foreachRDD(v -> {
                List<String> values = v.collect();
                values.stream().forEach(item -> {
                    System.out.println(item);
                    logger.info(item);
                });

            });
            //打印此次批处理取数量个数
            JavaDStream<Long> count = lines.count();
            count = count.map(x -> {
                logger.info("这次批一共多少条数据：{}", x);
                return x;
            });
            count.print();
            jsc.start();
            jsc.awaitTermination();
            jsc.stop();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
