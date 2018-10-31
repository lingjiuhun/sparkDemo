package com.cz.example.sparkDemo.config.kafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;
/**
 * @Author: caozhen
 * @Description: kafka数据 sparkStream 消费任务执行
 * @date: 2018/10/31 16:29
 */
@Component
public class SparkKafkaStreamExecutor implements Serializable,Runnable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(SparkKafkaStreamExecutor.class);
	
	@Value("${spark.stream.kafka.durations}")
	private String streamDurationTime;
	@Value("${kafka.broker.list}")
	private String metadatabrokerlist;
	@Value("${spark.kafka.topics}")
	private String topicsAll;
//	@Autowired
//	private transient Gson gson;

	private transient JavaStreamingContext jsc;
	@Autowired 
	private transient JavaSparkContext javaSparkContext;
	
	@Override
	public void run() {
		startStreamTask();
	}
	
	public void startStreamTask() {
		Set<String> topics = new HashSet<String>(Arrays.asList(topicsAll.split(",")));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", metadatabrokerlist);
		
		jsc = new JavaStreamingContext(javaSparkContext,
				Durations.seconds(Integer.valueOf(streamDurationTime)));
		jsc.checkpoint("checkpoint"); //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复

		// 得到数据流
		final JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(jsc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		System.out.println("stream started!");
		stream.print();
		stream.foreachRDD(v -> {
			//流式数据收集处理
			List<String> topicDatas = v.values().collect();
			topicDatas.parallelStream().forEach(m ->{
				System.out.println(m);
			});
			log.info("一批次数据流处理完： {}",topicDatas);
		});
		//优化：为每个分区创建一个连接
//		stream.foreachRDD(t->{
//			t.foreachPartition(f->{
//				while(f.hasNext()) {
//					Map<String, Object> symbolLDAHandlered =LDAModelPpl
//							.LDAHandlerOneArticle(sparkSession, SymbolAndNews.symbolHandlerOneArticle(sparkSession, f.next()._2));
//				}
//			});
//		});
		jsc.start();
	}

	public void destoryStreamTask() {
		if(jsc!=null) {
			jsc.stop();
		}		
	}

}
