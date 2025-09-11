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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import static org.apache.rocketmq.broker.longpolling.PollingResult.NOT_POLLING;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_FULL;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_SUC;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_TIMEOUT;

public class PopLongPollingService extends ServiceThread {

    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final NettyRequestProcessor processor;
    private final Cache<String, ConcurrentHashMap<String, Byte>> topicCidMap;
    private final Cache<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    private long lastCleanTime = 0;

    private final AtomicLong totalPollingNum = new AtomicLong(0);
    private final boolean notifyLast;

    public PopLongPollingService(BrokerController brokerController, NettyRequestProcessor processor,
        boolean notifyLast) {
        this.brokerController = brokerController;
        this.processor = processor;
        // 100000 topic default,  100000 lru topic + cid + qid
        this.topicCidMap = Caffeine.newBuilder()
            .maximumSize(this.brokerController.getBrokerConfig().getPopPollingMapSize() * 2L)
            .expireAfterAccess(this.brokerController.getBrokerConfig().getPopPollingMapExpireTimeSeconds(), TimeUnit.SECONDS)
            .build();

        this.pollingMap = Caffeine.newBuilder()
            .maximumSize(this.brokerController.getBrokerConfig().getPopPollingMapSize())
            .expireAfterAccess(this.brokerController.getBrokerConfig().getPopPollingMapExpireTimeSeconds(), TimeUnit.SECONDS)
            .build();
        this.notifyLast = notifyLast;
    }

    @Override
    public String getServiceName() {
        if (brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopLongPollingService.class.getSimpleName();
        }
        return PopLongPollingService.class.getSimpleName();
    }

    @Override
    public void run() {
        int i = 0;
        while (!this.stopped) {
            try {
                this.waitForRunning(20);
                i++;
                if (pollingMap.estimatedSize() == 0) {
                    continue;
                }
                long tmpTotalPollingNum = 0;
                for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.asMap().entrySet()) {
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
                                POP_LOGGER.info("polling, add fail again: {}", first);
                            }
                        }
                        if (brokerController.getBrokerConfig().isEnablePopLog()) {
                            POP_LOGGER.info("timeout , wakeUp polling : {}", first);
                        }
                        totalPollingNum.decrementAndGet();
                        wakeUp(first);
                    }
                    while (true);
                    if (i >= 100) {
                        long tmpPollingNum = popQ.size();
                        tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                        if (tmpPollingNum > 100) {
                            POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
                        }
                    }
                }

                if (i >= 100) {
                    POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}",
                        pollingMap.estimatedSize(), tmpTotalPollingNum, totalPollingNum.get(),
                        Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
                    totalPollingNum.set(tmpTotalPollingNum);
                    i = 0;
                }

                // clean unused
                if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 5 * 60 * 1000) {
                    cleanUnusedResource();
                }
            } catch (Throwable e) {
                POP_LOGGER.error("checkPolling error", e);
            }
        }
        // clean all;
        try {
            for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.asMap().entrySet()) {
                ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                PopRequest first;
                while ((first = popQ.pollFirst()) != null) {
                    wakeUp(first);
                }
            }
        } catch (Throwable e) {
        }
    }

    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId) {
        this.notifyMessageArrivingWithRetryTopic(topic, queueId, -1L, null, 0L, null, null);
    }

    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
        if (topic.startsWith(prefix)) {
            String originTopic = properties.get(MessageConst.PROPERTY_ORIGIN_TOPIC);
            String suffix = "_" + originTopic;
            String cid = topic.substring(prefix.length(), topic.length() - suffix.length());
            POP_LOGGER.info("Processing retry topic: {}, originTopic: {}, properties: {}",
                    topic, originTopic, properties);
            POP_LOGGER.info("Extracted cid: {} from retry topic: {}", cid, topic);
            long interval = brokerController.getBrokerConfig().getPopLongPollingForceNotifyInterval();
            boolean force = interval > 0L && offset % interval == 0L;
            if (queueId >= 0) {
                notifyMessageArriving(originTopic, -1, cid, force, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            notifyMessageArriving(originTopic, queueId, cid, force, tagsCode, msgStoreTime, filterBitMap, properties);
        } else {
            notifyMessageArriving(topic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
        }
    }
    
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        ConcurrentHashMap<String, Byte> cids = topicCidMap.getIfPresent(topic);
        if (cids == null) {
            return;
        }
        long interval = brokerController.getBrokerConfig().getPopLongPollingForceNotifyInterval();
        boolean force = interval > 0L && offset % interval == 0L;
        for (Map.Entry<String, Byte> cid : cids.entrySet()) {
            if (queueId >= 0) {
                notifyMessageArriving(topic, -1, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            notifyMessageArriving(topic, queueId, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
        }
    }

    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, false, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }

    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, force, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }

    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties, CommandCallback callback) {
        ConcurrentSkipListSet<PopRequest> remotingCommands = pollingMap.getIfPresent(KeyBuilder.buildPollingKey(topic, cid, queueId));
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }

        PopRequest popRequest = pollRemotingCommands(remotingCommands);
        if (popRequest == null) {
            return false;
        }

        if (!force && popRequest.getMessageFilter() != null && popRequest.getSubscriptionData() != null) {
            boolean match = popRequest.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
            if (match && properties != null) {
                match = popRequest.getMessageFilter().isMatchedByCommitLog(null, properties);
            }
            if (!match) {
                remotingCommands.add(popRequest);
                totalPollingNum.incrementAndGet();
                return false;
            }
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("lock release, new msg arrive, wakeUp: {}", popRequest);
        }

        return wakeUp(popRequest, callback);
    }

    public boolean wakeUp(final PopRequest request) {
        return wakeUp(request, null);
    }

    public boolean wakeUp(final PopRequest request, CommandCallback callback) {
        if (request == null || !request.complete()) {
            return false;
        }

        if (callback != null && request.getRemotingCommand() != null) {
            if (request.getRemotingCommand().getCallbackList() == null) {
                request.getRemotingCommand().setCallbackList(new ArrayList<>());
            }
            request.getRemotingCommand().getCallbackList().add(callback);
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
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (Exception e1) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
            }
        };

        this.brokerController.getPullMessageExecutor().submit(
            new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * @param ctx
     * @param remotingCommand
     * @param requestHeader
     * @return
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader) {
        return this.polling(ctx, remotingCommand, requestHeader, null, null);
    }

    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        if (requestHeader.getPollTime() <= 0 || this.isStopped()) {
            return NOT_POLLING;
        }
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic(), key -> new ConcurrentHashMap<>());
        cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        final PopRequest request = new PopRequest(remotingCommand, ctx, expired, subscriptionData, messageFilter);
        boolean isFull = totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            return POLLING_FULL;
        }
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }
        String key = KeyBuilder.buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId());
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(key, k -> new ConcurrentSkipListSet<>(PopRequest.COMPARATOR));
        int size = queue.size();
        if (size > brokerController.getBrokerConfig().getPopPollingSize()) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
            return POLLING_FULL;
        }

        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            totalPollingNum.incrementAndGet();
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
            }
            return POLLING_SUC;
        } else {
            POP_LOGGER.info("polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    public Cache<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return pollingMap;
    }

    public Cache<String, ConcurrentHashMap<String, Byte>> getTopicCidMap() {
        return topicCidMap;
    }

    private void cleanUnusedResource() {
        try {
            {
                Iterator<Map.Entry<String, ConcurrentHashMap<String, Byte>>> topicCidMapIter = topicCidMap.asMap().entrySet().iterator();
                while (topicCidMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentHashMap<String, Byte>> entry = topicCidMapIter.next();
                    String topic = entry.getKey();
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in topicCidMap!", topic);
                        topicCidMapIter.remove();
                        continue;
                    }
                    Iterator<Map.Entry<String, Byte>> cidMapIter = entry.getValue().entrySet().iterator();
                    while (cidMapIter.hasNext()) {
                        Map.Entry<String, Byte> cidEntry = cidMapIter.next();
                        String cid = cidEntry.getKey();
                        if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                            POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in topicCidMap!", cid, topic);
                            cidMapIter.remove();
                        }
                    }
                }
            }

            {
                Iterator<Map.Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIter = pollingMap.asMap().entrySet().iterator();
                while (pollingMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIter.next();
                    if (entry.getKey() == null) {
                        continue;
                    }
                    String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
                    if (keyArray.length != 3) {
                        continue;
                    }
                    String topic = keyArray[0];
                    String cid = keyArray[1];
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in pollingMap!", topic);
                        pollingMapIter.remove();
                        continue;
                    }
                    if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                        POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in pollingMap!", cid, topic);
                        pollingMapIter.remove();
                    }
                }
            }
        } catch (Throwable e) {
            POP_LOGGER.error("cleanUnusedResource", e);
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
            totalPollingNum.decrementAndGet();
        } while (popRequest != null && !popRequest.getChannel().isActive());

        return popRequest;
    }
}
