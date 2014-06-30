package com.alibaba.rocketmq.broker.transaction;

import java.util.List;


/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {
    public boolean open();


    public void close();


    public boolean put(final List<TransactionRecord> trs);


    public void remove(final List<Long> pks);


    public List<TransactionRecord> traverse(final long pk, final int nums);


    public long totalRecords();


    public long minPK();


    public long maxPK();
}
