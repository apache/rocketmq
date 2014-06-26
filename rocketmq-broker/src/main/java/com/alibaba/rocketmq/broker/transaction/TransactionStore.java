package com.alibaba.rocketmq.broker.transaction;

import java.util.List;


/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {
    public void open();


    public void close();


    public boolean write(final TransactionRecord tr);


    public void remove(final long pk);


    public void remove(final List<Long> pks);


    public List<TransactionRecord> traverse(final long pk, final int nums);


    public long totalRecords();


    public long maxOffset();
}
