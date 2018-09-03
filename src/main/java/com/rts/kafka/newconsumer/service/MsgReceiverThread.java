package com.rts.kafka.newconsumer.service;

import com.rts.kafka.newconsumer.multithreads.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MsgReceiverThread extends Daemon {

    private static final Logger logger = LoggerFactory.getLogger(MsgReceiverThread.class);
    private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue = new LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>>();
    private Map<String, Object> consumerConfig;
    private String alarmTopic;
    private ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks;
    private ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads;


    public MsgReceiverThread(Map<String, Object> consumerConfig, String alarmTopic,
                       ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks,
                       ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads) {

        super("thread-name");

        this.consumerConfig = consumerConfig;
        this.alarmTopic = alarmTopic;
        this.recordProcessorTasks = recordProcessorTasks;
        this.recordProcessorThreads = recordProcessorThreads;

    }


    @Override
    protected void runOneCycle() {

        //kafka Consumer是非线程安全的,所以需要每个线程建立一个consumer：外部一共创建了3个线程，所以需要有三个consumer
        KafkaConsumer<String, ByteBuffer> consumer = new KafkaConsumer<String, ByteBuffer>(consumerConfig);

        consumer.subscribe(Arrays.asList(alarmTopic));

        //检查线程中断标志是否设置, 如果设置则表示外界想要停止该任务,终止该任务
        try {
//            while (!Thread.currentThread().isInterrupted()) {
            while (true) {

                if (isStop.get()) {
                    if (!commitQueue.isEmpty()) {
                        for (int i= 0;i<commitQueue.size();i++) {
                            Map<TopicPartition, OffsetAndMetadata> toCommit = commitQueue.poll();
                            consumer.commitSync(toCommit);
                        }
                    }
                    break;
                }

                try {
                    //查看该消费者是否有需要提交的偏移信息, 使用非阻塞读取： 此处是需要提交的offset
                    Map<TopicPartition, OffsetAndMetadata> toCommit = commitQueue.poll();
                    if (toCommit != null) {
                        logger.debug("commit TopicPartition offset to kafka: " + toCommit);
                        consumer.commitSync(toCommit);
                    }


                    //最多轮询100ms：消费kafka的数据，每次获得一批数据，对这批数据
                    ConsumerRecords<String, ByteBuffer> records = consumer.poll(100);

                    if (records.isEmpty()) {
                        continue;
                    }
                    if (records.count() > 0) {
                        logger.debug("poll records size: " + records.count());
                    }

                    /**
                     * 遍历取出来的这一批数据，对这一批数据处理：record_processor线程只处理一个分区的消息，
                     * 同一个线程下多个分区的的record放到同一个commitQueue中
                     */
                    for (final ConsumerRecord<String, ByteBuffer> record : records) {
                        String topic = record.topic();
                        int partition = record.partition();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        RecordProcessor recordProcessorTask = recordProcessorTasks.get(topicPartition);

                        //如果当前分区还没有开始消费, 则就没有消费任务在map中， 新创建一个task和一个线程
                        if (recordProcessorTask == null) {

                            //生成新的处理任务和线程, 然后将其放入对应的map中进行保存：初始化 queue
                            recordProcessorTask = new RecordProcessor(commitQueue);
                            recordProcessorTasks.put(topicPartition, recordProcessorTask);
                            /**
                             * 启动一个线程
                             */
                            Thread thread = new Thread(recordProcessorTask);
                            thread.setName("Thread-for " + topicPartition.toString());
                            logger.info("start a Thread: " + thread.getName());
                            thread.start();
                            recordProcessorThreads.put(topicPartition, thread);

                        }

                        //将消息放到 该分区对应task的queue中，进行处理： 在处理每条数据的同时，还会进一步的处理相关相关的offset
                        recordProcessorTask.addRecordToQueue(record);

                    }

                } catch (Exception e) {

                    e.printStackTrace();
                    logger.warn("MsgReceiver exception " + e + " ignore it");

                }
            }
        } finally {
            consumer.close();
        }


    }
}
