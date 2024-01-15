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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.TransactionMetrics;
import org.apache.rocketmq.broker.transaction.TransactionMetrics.Metric;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@RunWith(MockitoJUnitRunner.class)
public class TransactionMetricsTest {
    private TransactionMetrics transactionMetrics;
    private String configPath;

    @Before
    public void setUp() throws Exception {
        configPath = "configPath";
        transactionMetrics = new TransactionMetrics(configPath);
    }

    /**
     * test addAndGet method
     */
    @Test
    public void testAddAndGet() {
        String topic = "testAddAndGet";
        int value = 10;
        long result = transactionMetrics.addAndGet(topic, value);

        assert result == value;
    }

    @Test
    public void testGetTopicPair() {
        String topic = "getTopicPair";
        Metric result = transactionMetrics.getTopicPair(topic);
        assert result != null;
    }

    @Test
    public void testGetTransactionCount() {
        String topicExist = "topicExist";
        String topicNotExist = "topicNotExist";

        transactionMetrics.addAndGet(topicExist, 10);

        assert transactionMetrics.getTransactionCount(topicExist) == 10;
        assert transactionMetrics.getTransactionCount(topicNotExist) == 0;
    }


    /**
     * test clean metrics
     */
    @Test
    public void testCleanMetrics() {
        String topic = "testCleanMetrics";
        int value = 10;
        assert transactionMetrics.addAndGet(topic, value) == value;
        transactionMetrics.cleanMetrics(Collections.singleton(topic));
        assert transactionMetrics.getTransactionCount(topic) == 0;
    }
}
