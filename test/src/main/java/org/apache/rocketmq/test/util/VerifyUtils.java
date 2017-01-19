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
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageExt;

public class VerifyUtils {
    private static Logger logger = Logger.getLogger(VerifyUtils.class);

    public static int verify(Collection<Object> sendMsgs, Collection<Object> recvMsgs) {
        int miss = 0;
        for (Object msg : sendMsgs) {
            if (!recvMsgs.contains(msg)) {
                miss++;
            }
        }

        return miss;
    }

    public static Collection<Object> getFilterdMessage(Collection<Object> sendMsgs,
        Collection<Object> recvMsgs) {
        Collection<Object> recvMsgsSync = Collections.synchronizedCollection(recvMsgs);
        Collection<Object> filterdMsgs = new ArrayList<Object>();
        int filterNum = 0;
        for (Object msg : recvMsgsSync) {
            if (sendMsgs.contains(msg)) {
                filterdMsgs.add(msg);
            } else {
                filterNum++;
            }
        }

        logger.info(String.format("[%s] messages is filterd!", filterNum));
        return filterdMsgs;
    }

    public static int verifyUserProperty(Collection<Object> sendMsgs, Collection<Object> recvMsgs) {
        return 0;
    }

    public static void verifyMessageQueueId(int expectId, Collection<Object> msgs) {
        for (Object msg : msgs) {
            MessageExt msgEx = (MessageExt) msg;
            assert expectId == msgEx.getQueueId();
        }
    }

    public static boolean verifyBalance(int msgSize, float error, int... recvSize) {
        boolean balance = true;
        int evenSize = msgSize / recvSize.length;
        for (int size : recvSize) {
            if (Math.abs(size - evenSize) > error * evenSize) {
                balance = false;
                break;
            }
        }
        return balance;
    }

    public static boolean verifyBalance(int msgSize, int... recvSize) {
        return verifyBalance(msgSize, 0.1f, recvSize);
    }

    public static boolean verifyDelay(long delayTimeMills, Collection<Object> recvMsgTimes,
        int errorMills) {
        boolean delay = true;
        for (Object timeObj : recvMsgTimes) {
            long time = (Long) timeObj;
            if (Math.abs(time - delayTimeMills) > errorMills) {
                delay = false;
                logger.info(String.format("delay error:%s", Math.abs(time - delayTimeMills)));
            }
        }
        return delay;
    }

    public static boolean verifyDelay(long delayTimeMills, Collection<Object> recvMsgTimes) {
        int errorMills = 500;
        return verifyDelay(delayTimeMills, recvMsgTimes, errorMills);
    }

    public static boolean verifyOrder(Collection<Collection<Object>> queueMsgs) {
        for (Collection<Object> msgs : queueMsgs) {
            if (!verifyOrderMsg(msgs)) {
                return false;
            }
        }
        return true;

    }

    public static boolean verifyOrderMsg(Collection<Object> msgs) {
        int min = Integer.MIN_VALUE;
        int curr;
        if (msgs.size() == 0 || msgs.size() == 1) {
            return true;
        } else {
            for (Object msg : msgs) {
                curr = Integer.parseInt((String) msg);
                if (curr < min) {
                    return false;
                } else {
                    min = curr;
                }
            }
        }
        return true;
    }

    public static boolean verifyRT(Collection<Object> rts, long maxRTMills) {
        boolean rtExpect = true;
        for (Object obj : rts) {
            long rt = (Long) obj;
            if (rt > maxRTMills) {
                rtExpect = false;
                logger.info(String.format("%s greater thran maxtRT:%s!", rt, maxRTMills));

            }
        }
        return rtExpect;
    }

    public static void main(String args[]) {
        verifyBalance(400, 0.1f, 230, 190);
    }
}
