package com.rts.kafka.newconsumer.demo2.service;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class Daemon extends Thread {

    Logger logger = Logger.getLogger(Daemon.class);


    private static final int DEFAULT_INTERVAL_SECONDS = 3; // 30 seconds

    protected AtomicBoolean isStop;

    private long intervalMs;

    {
        setDaemon(true);
    }


    public Daemon() {
        super();
        intervalMs = DEFAULT_INTERVAL_SECONDS  * 1000L;
        isStop = new AtomicBoolean(false);
    }

    public Daemon(String name) {
        this(name, DEFAULT_INTERVAL_SECONDS  * 1000L);
    }

    public Daemon(String name, long intervalMs) {
        this(intervalMs);
        this.setName(name);
    }

    public Daemon(long intervalMs) {
        this();
        this.intervalMs = intervalMs;
    }


    public void exit() {
        isStop.set(true);
    }

    @Override
    public void run() {
        runOneCycle();

    }

    // 在子类实现
    protected void runOneCycle() {

    }


}