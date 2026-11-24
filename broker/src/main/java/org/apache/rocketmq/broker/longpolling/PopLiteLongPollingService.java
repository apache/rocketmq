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

package org.apache.rocketmq.broker.longpolling;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.ChannelHandlerContext;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService.PollingQueue;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.broker.longpolling.PollingResult.NOT_POLLING;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_FULL;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_SUC;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_TIMEOUT;

/**
 * Long polling service specifically designed for lite consumption.
 * Stores pending requests in memory using clientId as the key instead of topic@cid@qid.
 * Notification and resource checking mechanisms are identical to those in PopLongPollingService.
 */
public class PopLiteLongPollingService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    private final BrokerController brokerController;
    private final NettyRequestProcessor processor;
    private final ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    private long lastCleanTime = 0;

    private final AtomicLong totalPollingNum = new AtomicLong(0);
    private final boolean notifyLast;

    public PopLiteLongPollingService(BrokerController brokerController, NettyRequestProcessor processor, boolean notifyLast) {
        this.brokerController = brokerController;
        this.processor = processor;
        this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        this.notifyLast = notifyLast;
    }

    @Override
    public String getServiceName() {
        if (brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopLiteLongPollingService.class.getSimpleName();
        }
        return PopLiteLongPollingService.class.getSimpleName();
    }

    @Override
    public void run() {
        int i = 0;
        while (!this.stopped) {
            try {
                this.waitForRunning(20);
                i++;
                if (pollingMap.isEmpty()) {
                    continue;
                }
                long tmpTotalPollingNum = 0;
                for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                    String key = entry.getKey();
                    ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                    if (popQ == null) {
                        continue;
                    }
                    PopRequest first;
                    do {
                        first = popQ.pollFirst();
                        if (first == null) {
                            break;
                        }
                        if (!first.isTimeout()) {
                            if (popQ.add(first)) {
                                break;
                            } else {
                                LOGGER.info("lite polling, add back again but failed. {}", first);
                            }
                        }
                        if (brokerController.getBrokerConfig().isEnablePopLog()) {
                            LOGGER.info("timeout , wakeUp lite polling : {}", first);
                        }
                        totalPollingNum.decrementAndGet();
                        wakeUp(first);
                    }
                    while (true);
                    if (i >= 100) {
                        long tmpPollingNum = popQ.size();
                        tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                        if (tmpPollingNum > 20) {
                            LOGGER.info("lite polling queue {} , size={} ", key, tmpPollingNum);
                        }
                    }
                }

                if (i >= 100) {
                    LOGGER.info("litePollingMapSize={}, tmpTotalSize={}, atomicTotalSize={}, diffSize={}",
                        pollingMap.size(), tmpTotalPollingNum, totalPollingNum.get(),
                        Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
                    totalPollingNum.set(tmpTotalPollingNum);
                    i = 0;
                }

                // clean unused
                if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 3 * 60 * 1000) {
                    cleanUnusedResource();
                }
            } catch (Throwable e) {
                LOGGER.error("checkLitePolling error", e);
            }
        }
        // clean all;
        try {
            for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                PopRequest first;
                while ((first = popQ.pollFirst()) != null) {
                    wakeUp(first);
                }
            }
        } catch (Throwable ignored) {
        }
    }

    public boolean notifyMessageArriving(final String clientId, boolean force, long msgStoreTime, String group) {
        String pollingKey = getPollingKey(clientId, group);
        PollingQueue queue = (PollingQueue) pollingMap.get(pollingKey);
        if (queue != null) {
            queue.notificationCount.incrementAndGet();
        }
        return notifyPollingRequests(queue);
    }

    private boolean notifyPollingRequests(ConcurrentSkipListSet<PopRequest> remotingCommands) {
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }
        PopRequest popRequest = pollRemotingCommands(remotingCommands);
        if (popRequest == null) {
            return false;
        }

        if (brokerController.getBrokerConfig().isEnableLitePopLog()) {
            LOGGER.info("notify lite polling, wakeUp: {}", popRequest);
        }
        return wakeUp(popRequest);
    }

    public boolean wakeUp(final PopRequest request) {
        if (request == null || !request.complete()) {
            return false;
        }
        if (!request.getCtx().channel().isActive()) {
            return false;
        }

        Runnable run = () -> {
            try {
                final RemotingCommand response = processor.processRequest(request.getCtx(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            LOGGER.error(request.toString());
                            LOGGER.error(response.toString());
                        }
                    }, brokerController.getBrokerMetricsManager().getRemotingMetricsManager());
                }
            } catch (Exception e) {
                LOGGER.error("ExecuteRequestWhenWakeup error.", e);
            }
        };

        this.brokerController.getPullMessageExecutor().submit(
            new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * Ensures the client's polling queue exists and snapshots its count before checking for messages or events.
     * The snapshot remains valid only while the counting queue remains in the map.
     *
     * @param clientId the client identifier
     * @param group the consumer group
     * @return the current notification count for the client
     */
    public long prepareNotificationSnapshot(String clientId, String group) {
        return ((PollingQueue) pollingMap.computeIfAbsent(getPollingKey(clientId, group), key -> new PollingQueue()))
            .notificationCount.get();
    }

    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        long bornTime, long pollTime, String clientId, String group) {
        return polling(ctx, remotingCommand, bornTime, pollTime, clientId, group, Long.MIN_VALUE);
    }

    /**
     * Registers a request and retries waiter notification if the count changed after the message check began.
     *
     * @param ctx the request channel context
     * @param remotingCommand the request command
     * @param bornTime the request creation time in milliseconds
     * @param pollTime the polling timeout in milliseconds
     * @param clientId the client identifier
     * @param group the consumer group
     * @param notifications the pre-check snapshot, or {@link Long#MIN_VALUE} to disable compensation
     * @return the polling registration result
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        long bornTime, long pollTime, String clientId, String group, long notifications) {
        if (pollTime <= 0 || this.isStopped()) {
            return NOT_POLLING;
        }
        long expired = bornTime + pollTime;
        final PopRequest request = new PopRequest(remotingCommand, ctx, expired, null, null);
        boolean isFull = totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            LOGGER.info("lite polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            return POLLING_FULL;
        }
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                LOGGER.info("lite polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }

        String pollingKey = getPollingKey(clientId, group);
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(pollingKey);
        if (queue == null) {
            queue = pollingMap.computeIfAbsent(pollingKey, key -> new PollingQueue());
        } else {
            // check size
            int size = queue.size();
            if (size > brokerController.getBrokerConfig().getPopPollingSize()) {
                LOGGER.info("lite polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
                return POLLING_FULL;
            }
        }
        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            totalPollingNum.incrementAndGet();
            if (brokerController.getBrokerConfig().isEnableLitePopLog()) {
                LOGGER.info("lite polling {}, result POLLING_SUC", remotingCommand);
            }
            if (notifications != Long.MIN_VALUE && notifications != ((PollingQueue) queue).notificationCount.get()) {
                notifyPollingRequests(queue);
            }
            return POLLING_SUC;
        } else {
            LOGGER.info("lite polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    private void cleanUnusedResource() {
        try {
            pollingMap.entrySet().removeIf(entry -> {
                if (CollectionUtils.isEmpty(entry.getValue())) {
                    LOGGER.info("clean polling structure of {}", entry.getKey()); // see getPollingKey()
                    return true;
                }
                return false;
            });
        } catch (Throwable ignored) {
        }
        lastCleanTime = System.currentTimeMillis();
    }

    private PopRequest pollRemotingCommands(ConcurrentSkipListSet<PopRequest> remotingCommands) {
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return null;
        }

        PopRequest popRequest;
        do {
            if (notifyLast) {
                popRequest = remotingCommands.pollLast();
            } else {
                popRequest = remotingCommands.pollFirst();
            }
            if (popRequest != null) {
                totalPollingNum.decrementAndGet();
            }
        } while (popRequest != null && !popRequest.getChannel().isActive());

        return popRequest;
    }

    // Assume that clientId is unique, so we use it as the key for now.
    private String getPollingKey(String clientId, String group) {
        return clientId;
    }
}
