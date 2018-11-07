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

package org.apache.rocketmq.broker;

import static org.apache.rocketmq.store.stats.BrokerStatsManager.TOPIC_PUT_NUMS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

public class BrokerStatsManagerTest {

    private BrokerStatsManager brokerStatsManager;
    private ThreadPoolExecutor executor;
    @Before
    public void init() {
        brokerStatsManager = new BrokerStatsManager("DefaultCluster");
        executor = new ThreadPoolExecutor(100, 200, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadFactoryImpl("testMultiThread"));
    }

    @Test
    public void test_getAndCreateStatsItem_multiThread() throws InterruptedException {

        for(int i =0; i < 10000; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    brokerStatsManager.incTopicPutNums("topicTest", 2, 1);
                }
            });
        }
        Thread.sleep(5000);
        System.out.println(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS,"topicTest").getValue());
    }
}