/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.broker.transaction.jdbc;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


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
