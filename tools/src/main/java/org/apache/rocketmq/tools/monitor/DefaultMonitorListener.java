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

package org.apache.rocketmq.tools.monitor;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DefaultMonitorListener implements MonitorListener {
    private final static String LOG_PREFIX = "[MONITOR] ";
    private final static String LOG_NOTIFY = LOG_PREFIX + " [NOTIFY] ";
    private final Logger logger = LoggerFactory.getLogger(DefaultMonitorListener.class);

    public DefaultMonitorListener() {
    }

    @Override
    public void beginRound() {
        logger.info("{}=========================================beginRound", LOG_PREFIX);
    }

    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        logger.info("{}reportUndoneMsgs: {}", LOG_PREFIX, undoneMsgs);
    }

    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        logger.info("{}reportFailedMsgs: {}", LOG_PREFIX, failedMsgs);
    }

    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        logger.info("{}reportDeleteMsgsEvent: {}", LOG_PREFIX, deleteMsgsEvent);
    }

    @Override
    public void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable) {
        if (criTable == null || criTable.isEmpty()) {
            logger.warn("{}ConsumerRunningInfo is empty.", LOG_NOTIFY);
            return;
        }

        ConsumerRunningInfo firstValue = criTable.firstEntry().getValue();
        if (firstValue == null || firstValue.getProperties() == null) {
            logger.warn("{}ConsumerRunningInfo entry is empty.", LOG_NOTIFY);
            return;
        }

        String consumerGroup = firstValue.getProperties().getProperty("consumerGroup");

        {
            boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
            if (!result) {
                logger.info("{}reportConsumerRunningInfo: ConsumerGroup: {}, Subscription different", LOG_NOTIFY, consumerGroup);
            }
        }

        {
            Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumerRunningInfo> next = it.next();
                String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                if (!result.isEmpty()) {
                    logger.info("{}reportConsumerRunningInfo: ConsumerGroup: {}, ClientId: {}, {}",
                            LOG_NOTIFY,
                            consumerGroup,
                            next.getKey(),
                            result);
                }
            }
        }
    }

    @Override
    public void endRound() {
        logger.info("{}=========================================endRound", LOG_PREFIX);
    }
}
