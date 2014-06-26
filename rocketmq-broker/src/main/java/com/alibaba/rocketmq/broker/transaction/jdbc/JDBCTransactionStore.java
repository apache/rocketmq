package com.alibaba.rocketmq.broker.transaction.jdbc;

import java.util.List;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;


public class JDBCTransactionStore implements TransactionStore {

    @Override
    public void open() {
    }


    @Override
    public void close() {
    }


    @Override
    public boolean write(TransactionRecord tr) {
        return false;
    }


    @Override
    public void remove(long pk) {
        // TODO Auto-generated method stub

    }


    @Override
    public void remove(List<Long> pks) {
        // TODO Auto-generated method stub

    }


    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long totalRecords() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long maxOffset() {
        // TODO Auto-generated method stub
        return 0;
    }

}
