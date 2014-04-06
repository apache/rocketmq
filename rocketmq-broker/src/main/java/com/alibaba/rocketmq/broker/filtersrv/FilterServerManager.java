package com.alibaba.rocketmq.broker.filtersrv;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;


public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final ConcurrentHashMap<String/* Filter Server Addr */, AtomicLong/* Timestamp */> filterServerTable =
            new ConcurrentHashMap<String, AtomicLong>();

    public static final long FilterServerMaxIdleTimeMills = 30000;


    public FilterServerManager() {
    }


    public void registerFilterServer(final String filterServerAddr) {
        AtomicLong timestamp = this.filterServerTable.get(filterServerAddr);
        if (timestamp != null) {
            timestamp.set(System.currentTimeMillis());
        }
        else {
            this.filterServerTable.put(filterServerAddr, new AtomicLong(System.currentTimeMillis()));
            log.info("Receive a New Filter Server<%s>", filterServerAddr);
        }
    }


    /**
     * Filter Server 10s向Broker注册一次，Broker如果发现30s没有注册，则删除它
     */
    public void scanExpiredFilterServer() {
        // 单位毫秒

        Iterator<Entry<String, AtomicLong>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, AtomicLong> next = it.next();
            long timestamp = next.getValue().get();
            if ((System.currentTimeMillis() - timestamp) > FilterServerMaxIdleTimeMills) {
                log.info("The Filter Server<%s> expired, remove it", next.getKey());
                it.remove();
            }
        }
    }
}
