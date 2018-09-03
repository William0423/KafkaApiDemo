package com.rts.kafka.consumer.simplethread;


import com.rts.common.Constant;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.List;

/**
 * kafka消息消费者，消费kafka指定topic的数据
 */
public class KafkaStringConsumer {
	private final ConsumerConnector consumer;

	private KafkaStringConsumer() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", "localhost:2181");
		// group 代表一个消费组
		props.put("group.id", Constant.KAFKA_GROUP);
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "1000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("rebalance.max.retries", "5");
		props.put("rebalance.backoff.ms", "1200");
		props.put("auto.commit.interval.ms", "1000");
//		props.put("auto.offset.reset", "largest");
		props.put("auto.offset.reset", "smallest");//largest

		//自动提交
		props.put("enable.auto.commit", "false");
//		props.put("auto.commit", false);
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);

		consumer = Consumer.createJavaConsumerConnector(config);
	}

	void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(Constant.KAFKA_TOPIC, new Integer(1));

		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);


		KafkaStream<String, String> stream = consumerMap.get(Constant.KAFKA_TOPIC).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext())
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" + it.next().message() + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}

	public static void main(String[] args) {
		new KafkaStringConsumer().consume();
	}
}
