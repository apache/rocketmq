package com.alibaba.rocketmq.broker.transaction.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;


public class JDBCTransactionStoreTest {

    @Test
    public void test_derby_open() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();
        config.setJdbcDriverClass("org.apache.derby.jdbc.EmbeddedDriver");
        config.setJdbcURL("jdbc:derby:xxx;create=true");
        config.setJdbcUser("xxx");
        config.setJdbcPassword("xxx");
        TransactionStore store = new JDBCTransactionStore(config);

        boolean open = store.open();
        System.out.println(open);
        Assert.assertTrue(open);
        store.close();
    }


    // @Test
    public void test_mysql_open() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config);

        boolean open = store.open();
        System.out.println(open);
        Assert.assertTrue(open);
        store.close();
    }


    // @Test
    public void test_mysql_put() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config);

        boolean open = store.open();
        System.out.println(open);
        Assert.assertTrue(open);

        long begin = System.currentTimeMillis();
        List<TransactionRecord> trs = new ArrayList<TransactionRecord>();
        for (int i = 0; i < 20; i++) {
            TransactionRecord tr = new TransactionRecord();
            tr.setOffset(i);
            tr.setProducerGroup("PG_" + i);
            trs.add(tr);
        }

        boolean write = store.put(trs);

        System.out.println("TIME=" + (System.currentTimeMillis() - begin));

        Assert.assertTrue(write);

        store.close();
    }


    // @Test
    public void test_mysql_remove() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config);

        boolean open = store.open();
        System.out.println(open);
        Assert.assertTrue(open);

        List<Long> pks = new ArrayList<Long>();
        pks.add(2L);
        pks.add(4L);
        pks.add(6L);
        pks.add(8L);
        pks.add(11L);

        store.remove(pks);

        store.close();
    }
}
