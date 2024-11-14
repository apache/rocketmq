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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private static long minInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.minInterval", "1000"));
    private final Logger log = LoggerFactory.getLogger(RebalanceService.class);
    private final MQClientInstance mqClientFactory;
    private ScheduledExecutorService scheduledExecutorService;
    private long lastRebalanceTimestamp = System.currentTimeMillis();

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        if (mqClientFactory.getClientConfig().getEnableRebalanceTransferInPop()) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    MessageRequestModeSerializeWrapper messageRequestModeSerializeWrapper = new MessageRequestModeSerializeWrapper();//mqClientFactory.getMQAdminImpl();
                    ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> msgRqCodes = messageRequestModeSerializeWrapper.getMessageRequestModeMap();
                    Set<String> groupSet = new HashSet<>();
                    for (Map.Entry<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> topicEntry : msgRqCodes.entrySet()) {
                        for (Map.Entry<String, SetMessageRequestModeRequestBody> groupEntry : topicEntry.getValue().entrySet()) {
                            mqClientFactory.updateRebalanceByBrokerAndClientMap(groupEntry.getKey(), topicEntry.getKey(),
                                    groupEntry.getValue().getMode(), groupEntry.getValue().getPopShareQueueNum());
                            groupSet.add(groupEntry.getKey());
                        }
                    }

                    for (String group : groupSet) {
                        mqClientFactory.setClientRebalance(true, group);
                    }
                }
            }, 1, TimeUnit.MINUTES);
        }

        long realWaitInterval = waitInterval;
        while (!this.isStopped()) {
            this.waitForRunning(realWaitInterval);

            long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
            if (interval < minInterval) {
                realWaitInterval = minInterval - interval;
            } else {
                boolean balanced = this.mqClientFactory.doRebalance();
                realWaitInterval = balanced ? waitInterval : minInterval;
                lastRebalanceTimestamp = System.currentTimeMillis();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
