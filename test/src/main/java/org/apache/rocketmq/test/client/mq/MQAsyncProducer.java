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

package org.apache.rocketmq.test.client.mq;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.util.TestUtil;

public class MQAsyncProducer {
    private static Logger logger = LoggerFactory.getLogger(MQAsyncProducer.class);
    private AbstractMQProducer producer = null;
    private long msgNum;
    private int intervalMills;
    private Thread sendT;
    private AtomicBoolean bPause = new AtomicBoolean(false);

    public MQAsyncProducer(final AbstractMQProducer producer, final long msgNum,
        final int intervalMills) {
        this.producer = producer;
        this.msgNum = msgNum;
        this.intervalMills = intervalMills;

        sendT = new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < msgNum; i++) {
                    if (!bPause.get()) {
                        producer.send();
                        TestUtil.waitForMoment(intervalMills);
                    } else {
                        while (true) {
                            if (bPause.get()) {
                                TestUtil.waitForMoment(10);
                            } else
                                break;
                        }
                    }

                }
            }
        });

    }

    public void start() {
        sendT.start();
    }

    public void waitSendAll(int waitMills) {
        long startTime = System.currentTimeMillis();
        while ((producer.getAllMsgBody().size() + producer.getSendErrorMsg().size()) < msgNum) {
            if (System.currentTimeMillis() - startTime < waitMills) {
                TestUtil.waitForMoment(200);
            } else {
                logger.error(String.format("time elapse:%s, but the message sending has not finished",
                    System.currentTimeMillis() - startTime));
                break;
            }
        }
    }

    public void pauseProducer() {
        bPause.set(true);
    }

    public void notifyProducer() {
        bPause.set(false);
    }

}
