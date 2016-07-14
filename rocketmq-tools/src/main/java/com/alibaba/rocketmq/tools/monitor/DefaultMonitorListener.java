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

package com.alibaba.rocketmq.tools.monitor;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;


public class DefaultMonitorListener implements MonitorListener {
    private final static String LogPrefix = "[MONITOR] ";
    private final static String LogNotify = LogPrefix + " [NOTIFY] ";
    private final Logger log = ClientLogger.getLog();


    public DefaultMonitorListener() {
    }


    @Override
    public void beginRound() {
        log.info(LogPrefix + "=========================================beginRound");
    }


    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        log.info(String.format(LogPrefix + "reportUndoneMsgs: %s", undoneMsgs));
    }


    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        log.info(String.format(LogPrefix + "reportFailedMsgs: %s", failedMsgs));
    }


    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        log.info(String.format(LogPrefix + "reportDeleteMsgsEvent: %s", deleteMsgsEvent));
    }


    @Override
    public void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable) {

        {
            boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
            if (!result) {
                log.info(String.format(LogNotify
                        + "reportConsumerRunningInfo: ConsumerGroup: %s, Subscription different", criTable
                        .firstEntry().getValue().getProperties().getProperty("consumerGroup")));
            }
        }


        {
            Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumerRunningInfo> next = it.next();
                String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                if (result != null && !result.isEmpty()) {
                    log.info(String.format(LogNotify
                                    + "reportConsumerRunningInfo: ConsumerGroup: %s, ClientId: %s, %s", //
                            criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup"),//
                            next.getKey(),//
                            result));
                }
            }
        }
    }


    @Override
    public void endRound() {
        log.info(LogPrefix + "=========================================endRound");
    }
}
