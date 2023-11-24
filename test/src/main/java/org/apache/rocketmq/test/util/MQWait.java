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

package org.apache.rocketmq.test.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;

import static com.google.common.truth.Truth.assertThat;

public class MQWait {
    private static Logger logger = LoggerFactory.getLogger(MQWait.class);

    public static boolean waitConsumeAll(int timeoutMills, Collection<Object> allSendMsgs,
        AbstractListener... listeners) {
        boolean recvAll = false;
        long startTime = System.currentTimeMillis();
        Collection<Object> noDupMsgs = new ArrayList<Object>();
        while (!recvAll) {
            if ((System.currentTimeMillis() - startTime) < timeoutMills) {
                noDupMsgs.clear();
                try {
                    for (AbstractListener listener : listeners) {
                        Collection<Object> recvMsgs = Collections
                            .synchronizedCollection(listener.getAllUndupMsgBody());
                        noDupMsgs.addAll(VerifyUtils.getFilterdMessage(allSendMsgs, recvMsgs));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    assertThat(noDupMsgs).containsAllIn(allSendMsgs);
                    recvAll = true;
                    break;
                } catch (Throwable e) {
                }
                TestUtil.waitForMoment(500);
            } else {
                logger.error(String.format(
                    "timeout but still not receive all messages,expectSize[%s],realSize[%s]",
                    allSendMsgs.size(), noDupMsgs.size()));
                break;
            }
        }

        return recvAll;
    }

    public static void setCondition(Condition condition, int waitTimeMills, int intervalMills) {
        long startTime = System.currentTimeMillis();
        while (!condition.meetCondition()) {
            if (System.currentTimeMillis() - startTime > waitTimeMills) {
                logger.error("time out,but contidion still not meet!");
                break;
            } else {
                TestUtil.waitForMoment(intervalMills);
            }
        }
    }

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        MQWait.setCondition(new Condition() {
            int i = 0;

            public boolean meetCondition() {
                i++;
                return i == 100;
            }
        }, 10 * 1000, 200);

    }

}
