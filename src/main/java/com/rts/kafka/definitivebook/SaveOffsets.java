package com.rts.kafka.definitivebook;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class SaveOffsets {


    private Map<String, Object> consumerConfig;
    private KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);

    private class SaveOffsetsOnRebalance  implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//            commitDBTransaction();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            for (TopicPartition partition : partitions) {
//                consumer.seek(partition, getOffsetFromDB(partition)); // getOffsetFromDB(partition)获取数据库的偏移量，并且
            }

        }

    }

    public void Consumer() {

        /**
         * 第一次订阅，进行以此seek()
         */
        consumer.subscribe(Arrays.asList("topics"), new SaveOffsetsOnRebalance());
        consumer.poll(0);

        /**
         * 进行第二次seek()
         */
        for (TopicPartition partition : consumer.assignment()) {
//            consumer.seek(partition, getOffsetFromDB(partition));
        }

//        consumer.assign();


        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)

            /**
             * 处理数据和保存数据在一个事物操作中
             */
            {
//                processRecord(record);
//                storeRecordInDB(record);
//                storeOffsetInDB(record.topic(), record.partition(), record.offset());
            }
//            commitDBTransaction();

        }

    }


}
