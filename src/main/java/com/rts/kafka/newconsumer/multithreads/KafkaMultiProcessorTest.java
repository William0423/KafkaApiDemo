package com.rts.kafka.newconsumer.multithreads;

import com.google.common.collect.ImmutableMap;
import com.rts.kafka.newconsumer.service.MsgReceiverThread;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

public class KafkaMultiProcessorTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMultiProcessorTest.class);
    //订阅的topic
    private String alarmTopic;
    //brokers地址
    private String servers;
    //消费group
    private String group;
    //kafka消费者配置
    private Map<String, Object> consumerConfig;
//    private Thread[] threads;
    private List<MsgReceiverThread> consumerThreadList = new ArrayList<MsgReceiverThread>();

    /**
     * 按照分区划分：
     */
    //保存处理任务和线程的map
    private ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        KafkaMultiProcessorTest test = new KafkaMultiProcessorTest();
        test.setAlarmTopic("newapi_topic");
//        test.setGroup("newapi_consumer_groupid_v1");
        test.setGroup("newapi_consumer_groupid");
        test.setServers("localhost:9092");
        //....省略设置topic和group的代码
        test.init();
    }

    public void init() {
        consumerConfig = getConsumerConfig();
        logger.debug("get kafka consumerConfig: " + consumerConfig.toString());
        //创建threadsNum个线程用于读取kafka消息, 且位于同一个group中, 这个topic有12个分区, 最多12个consumer进行读取
        int threadsNum = 3;
        logger.debug("create " + threadsNum + " threads to consume kafka warn msg");
//        threads = new Thread[threadsNum];

        for (int i = 0; i < threadsNum; i++) {
            /**
             * 此处启动三个线程，相当于三个消费中； 每个线程内部，即每个消费中内容，又有新的线程，这些线程是根据分区划分的。
             */
            MsgReceiverThread msgReceiver = new MsgReceiverThread(consumerConfig, alarmTopic, recordProcessorTasks, recordProcessorThreads);
//            Thread thread = new Thread(msgReceiver);
//            threads[i] = thread;
//            thread.setName("alarm msg consumer " + i);
            consumerThreadList.add(msgReceiver);
        }

        //启动这几个线程
        for (int i = 0; i < threadsNum; i++) {
            consumerThreadList.get(i).start();
        }

        logger.debug("finish creating" + threadsNum + " threads to consume kafka warn msg");

    }

    //销毁启动的线程
    public void destroy() {
        closeRecordProcessThreads();
        closeKafkaConsumer();
    }

    private void closeRecordProcessThreads() {
        logger.debug("start to interrupt record process threads");
//        for (Map.Entry<TopicPartition, Thread> entry : recordProcessorThreads.entrySet()) {
//            Thread thread = entry.getValue();
//            thread.interrupt();
//        }

        for (Map.Entry<TopicPartition, RecordProcessor> entry : recordProcessorTasks.entrySet()) {
            RecordProcessor task = entry.getValue();
            task.stopRecordProcessor();

        }
        logger.debug("finish interrupting record process threads");
    }

    private void closeKafkaConsumer() {
        logger.debug("start to interrupt kafka consumer threads");
        //使用interrupt中断线程, 在线程的执行方法中已经设置了响应中断信号
        for (int i = 0; i < consumerThreadList.size(); i++) {
            consumerThreadList.get(i).exit();
        }
        logger.debug("finish interrupting consumer threads");
    }

    //kafka consumer配置
    private Map<String, Object> getConsumerConfig() {
        return ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", servers)
                .put("group.id", group)
                .put("auto.offset.reset", "earliest")
                .put("enable.auto.commit", "false")
                .put("session.timeout.ms", "30000")
                .put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .put("max.poll.records", 1000)
                .build();
    }

    public void setAlarmTopic(String alarmTopic) {
        this.alarmTopic = alarmTopic;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
