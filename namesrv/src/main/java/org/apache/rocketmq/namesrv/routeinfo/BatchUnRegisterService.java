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

package org.apache.rocketmq.namesrv.routeinfo;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * BatchUnRegisterProcessor provides a mechanism to unregister broker in batch manner, which can speed up broker offline
 * process.
 */
public class BatchUnRegisterService extends ServiceThread {
    private final RouteInfoManager routeInfoManager;
    private BlockingQueue<UnRegisterBrokerRequestHeader> unRegisterQueue;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    public BatchUnRegisterService(RouteInfoManager routeInfoManager, NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        this.unRegisterQueue = new LinkedBlockingQueue<>(namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    /**
     * Submits an unregister request to this queue.
     *
     * @param unRegisterRequest the request to submit
     * @return {@code true} if the request was added to this queue, else {@code false}
     */
    public boolean submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return unRegisterQueue.offer(unRegisterRequest);
    }

    @Override
    public String getServiceName() {
        return BatchUnRegisterService.class.getName();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                final UnRegisterBrokerRequestHeader request = unRegisterQueue.poll(3, TimeUnit.SECONDS);
                if (request != null) {
                    Set<UnRegisterBrokerRequestHeader> unRegisterRequests = new HashSet<>();
                    unRegisterQueue.drainTo(unRegisterRequests);

                    // Add polled request
                    unRegisterRequests.add(request);

                    this.routeInfoManager.unRegisterBroker(unRegisterRequests);
                }
            } catch (Throwable e) {
                log.error("Handle unregister broker request failed", e);
            }
        }
    }

    // For test only
    int queueLength() {
        return this.unRegisterQueue.size();
    }
}
