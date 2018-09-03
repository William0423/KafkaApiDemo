package com.rts.kafka.producer;

import java.util.*;

import com.rts.common.Constant;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka消息生产者：生成数据到指定的kafka的topic
 */
public class KafkaProducer {
	private final Producer<String, String> producer;

	private KafkaProducer() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", "localhost:9092");
		props.put("zk.connect", "localhost:2181");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	/**
	 * 生产信息
	 */
	void produce() {
		int no = 520;
		final int COUNT = 540;
		List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String, String>>();
		while (no < COUNT) {
			Map<String, Object> map = new HashMap<String, Object>();
			String key = String.valueOf(no);

			String value = "message" + no;

			no++;
			list.add(new KeyedMessage<String, String>(Constant.KAFKA_TOPIC, key, value));
		}
		producer.send(list);
	}

	public static void main(String[] args) {
		new KafkaProducer().produce();
	}
}
