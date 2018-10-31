package com.cz.example.sparkDemo.config;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义接收streaming类
 */
public class CustomReceiver extends Receiver<String> {

    private static Logger logger = LoggerFactory.getLogger(CustomReceiver.class);


    /**
     *
     * @author
     * @date
     * @version
     */
    private static final long serialVersionUID = 5817531198342629801L;

    public CustomReceiver(StorageLevel storageLevel) {
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
            int value = RandomUtils.nextInt(100);
            if(value <20) {
                try {
                    Thread.sleep(1000);
                }catch (Exception e) {
                    logger.error("sleep exception",e);
                    restart("sleep exception", e);
                }
            }
            //存入数据
            store(String.valueOf(value));
        }
    }


    @Override
    public void onStop() {
        logger.info("即将停止Receiver...");
    }

}
