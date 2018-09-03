package com.rts.kafka.newconsumer.simpleThread;

import com.rts.common.Constant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStringConsumerNew {



    public static void consumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", Constant.KAFKA_GROUP);
        //自动提交
        props.put("enable.auto.commit", "true");
        //自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //key和value的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(Constant.KAFKA_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }

    public static void main(String[] args) {
        consumer();
    }


}
