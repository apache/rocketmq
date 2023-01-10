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

package org.apache.rocketmq.test.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.clientinterface.MQCollector;
import org.apache.rocketmq.test.util.TestUtil;

public class AbstractListener extends MQCollector implements MessageListener {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractListener.class);
    protected boolean isDebug = true;
    protected String listenerName = null;
    protected Collection<Object> allSendMsgs = null;

    public AbstractListener() {
        super();
    }

    public AbstractListener(String listenerName) {
        super();
        this.listenerName = listenerName;
    }

    public AbstractListener(String originMsgCollector, String msgBodyCollector) {
        super(originMsgCollector, msgBodyCollector);
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    public void waitForMessageConsume(int timeoutMills) {
        TestUtil.waitForMonment(timeoutMills);
    }

    public void stopRecv() {
        super.lockCollectors();
    }

    public Collection<Object> waitForMessageConsume(Collection<Object> allSendMessages, int timeoutMills) {
        this.allSendMsgs = allSendMessages;
        List<Object> sendMessages = new ArrayList<>(allSendMessages);

        long curTime = System.currentTimeMillis();
        while (!sendMessages.isEmpty()) {
            sendMessages.removeIf(msg -> msgBodys.getAllData().contains(msg));
            if (sendMessages.isEmpty()) {
                break;
            } else {
                if (System.currentTimeMillis() - curTime >= timeoutMills) {
                    LOGGER.error(String.format("timeout but [%s] not recv all send messages!", listenerName));
                    break;
                } else {
                    LOGGER.info(String.format("[%s] still [%s] msg not recv!", listenerName, sendMessages.size()));
                    TestUtil.waitForMonment(500);
                }
            }
        }
        return sendMessages;
    }

    public long waitForMessageConsume(int size, int timeoutMills) {
        long curTime = System.currentTimeMillis();
        while (true) {
            if (msgBodys.getDataSize() >= size) {
                break;
            }
            if (System.currentTimeMillis() - curTime >= timeoutMills) {
                LOGGER.error(String.format("timeout but  [%s]  not recv all send messages!", listenerName));
                break;
            } else {
                LOGGER.info(String.format("[%s] still [%s] msg not recv!",
                    listenerName, size - msgBodys.getDataSize()));
                TestUtil.waitForMonment(500);
            }
        }

        return msgBodys.getDataSize();
    }

    public void waitForMessageConsume(Map<Object, Object> sendMsgIndex, int timeoutMills) {
        Collection<Object> notRecvMsgs = waitForMessageConsume(sendMsgIndex.keySet(), timeoutMills);
        for (Object object : notRecvMsgs) {
            LOGGER.info("{}", sendMsgIndex.get(object));
        }
    }
}
