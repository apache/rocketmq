/*
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

package org.apache.rocketmq.test.client.producer.transaction;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQTransactionalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMsgIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TransactionalMsgIT.class);
    private RMQTransactionalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getTransactionalProducer(nsAddr, topic, new TransactionListenerImpl());
        consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener());
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testMessageVisibility() throws Exception {
        Thread.sleep(3000);
        int msgSize = 120;
        List<Object> msgs = MQMessageFactory.getMsg(topic, msgSize);
        for (int i = 0; i < msgSize; i++) {
            producer.send(msgs.get(i), getTransactionHandle(i));
        }
        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(), consumer.getListener());
        assertThat(recvAll).isEqualTo(true);
    }

    static Pair<Boolean, LocalTransactionState> getTransactionHandle(int msgIndex) {
        switch (msgIndex % 5) {
            case 0:
                //commit immediately
                return new Pair<>(true, LocalTransactionState.COMMIT_MESSAGE);
            case 1:
                //rollback immediately
                return new Pair<>(true, LocalTransactionState.ROLLBACK_MESSAGE);
            case 2:
                //commit in check
                return new Pair<>(false, LocalTransactionState.COMMIT_MESSAGE);
            case 3:
                //rollback in check
                return new Pair<>(false, LocalTransactionState.ROLLBACK_MESSAGE);
            case 4:
            default:
                return new Pair<>(false, LocalTransactionState.UNKNOW);

        }
    }

    static private class TransactionListenerImpl implements TransactionListener {
        ConcurrentHashMap<String, LocalTransactionState> checkStatus = new ConcurrentHashMap<>();

        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            Pair<Boolean, LocalTransactionState> transactionHandle = (Pair<Boolean,LocalTransactionState>) arg;
            if (transactionHandle.getObject1()) {
                return transactionHandle.getObject2();
            } else {
                checkStatus.put(msg.getTransactionId(), transactionHandle.getObject2());
                return LocalTransactionState.UNKNOW;
            }
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            LocalTransactionState state = checkStatus.get(msg.getTransactionId());
            if (state == null) {
                return LocalTransactionState.UNKNOW;
            } else {
                return state;
            }
        }
    }
}
