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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PullNotifyQueue.PullNotifyQueueConfig;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.StoreStatsService;

public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final String TOPIC_QUEUEID_SEPARATOR = "@";
    protected final BrokerController brokerController;
    protected final ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    private final PullNotifyQueueConfig notifyQueueConfig;
    private final PullNotifyQueue<NotifyData> notifyQueue;
    private final long checkInterval;
    private long lastCheckHoldRequestNanoTime = System.nanoTime();
    private long lastRefreshTime = System.nanoTime();

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
            this.checkInterval = 5L * 1000 * 1000 * 1000;
        } else {
            this.checkInterval = 1L * this.brokerController.getBrokerConfig().getShortPollingTimeMills() * 1000 * 1000;
        }
        this.notifyQueueConfig = new PullNotifyQueueConfig();
        refreshNotifyQueueConfig(null);
        notifyQueue = new PullNotifyQueue<>(notifyQueueConfig);
    }

    private void refreshNotifyQueueConfig(StoreStatsService statsService) {
        this.notifyQueueConfig.setTpsThreshold(brokerController.getBrokerConfig().getPullNotifyTpsThreshold());
        this.notifyQueueConfig.setMaxBatchSize(brokerController.getBrokerConfig().getPullNotifyMaxBatchSize());
        this.notifyQueueConfig.setMaxLatencyNs(
                1_000_000L * brokerController.getBrokerConfig().getPullNotifyMaxLatencyMs());
        this.notifyQueueConfig.setSuggestAvgBatchEachQueue(
                brokerController.getBrokerConfig().getPullNotifySuggestAvgBatchEachQueue());

        if (statsService != null) {
            try {
                String s = statsService.getPutTps(5);
                boolean enable1 = notifyQueueConfig.getTps() > notifyQueueConfig.getTpsThreshold();
                if (StringUtils.isNotEmpty(s)) {
                    this.notifyQueueConfig.setTps((int) Double.parseDouble(s));
                }
                boolean enable2 = notifyQueueConfig.getTps() > notifyQueueConfig.getTpsThreshold();
                if (!enable1 && enable2) {
                    log.info("PullRequestHoldService start flow control, current tps:{}", notifyQueueConfig.getTps());
                } else if (enable1 && !enable2) {
                    log.info("PullRequestHoldService stop flow control, current tps:{}", notifyQueueConfig.getTps());
                }
                if (enable2) {
                    log.info("PullRequestHoldService current activeConsumeQueueCount: {}, lastBatchDrainTime {}ms",
                            notifyQueue.getActiveConsumeQueueCount(),
                            notifyQueue.getLastBatchDrainTime() / 1000 / 1000);
                }
            } catch (NoSuchElementException e) {
                log.info("statsService.getPutTps fail, maybe just started, ignore.");
            } catch (Exception e) {
                log.warn("refreshNotifyQueueConfig fail: {}", e.toString());
            }
        }
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder(topic.length() + 5);
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            long beginTime = System.nanoTime();
            if (beginTime - this.lastCheckHoldRequestNanoTime > this.checkInterval) {
                this.checkHoldRequest();
                this.lastCheckHoldRequestNanoTime = System.nanoTime();

                long costMillis = (this.lastCheckHoldRequestNanoTime - beginTime) / 1000 / 1000;
                if (costMillis > 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costMillis);
                }
            }
            if (beginTime - lastRefreshTime > 5_000_000_000L) {
                MessageStore store = brokerController.getMessageStore();
                if (store instanceof DefaultMessageStore) {
                    StoreStatsService statsService = ((DefaultMessageStore) store).getStoreStatsService();
                    refreshNotifyQueueConfig(statsService);
                }
                this.lastRefreshTime = beginTime;
            }


            try {
                LinkedList<NotifyData> list = notifyQueue.drain();
                if (list.size() > 0) {
                    Map<String, List<NotifyData>> groups =
                            list.stream().collect(Collectors.groupingBy(d -> buildKey(d.topic, d.queueId)));
                    notifyQueue.updateActiveConsumeQueueCount(groups.size());
                    for (Entry<String, List<NotifyData>> entry : groups.entrySet()) {
                        List<NotifyData> groupData = entry.getValue();
                        if (groupData.size() == 0) {
                            continue;
                        }
                        long maxOffset = groupData.stream().mapToLong(d -> d.maxOffset).max().orElse(0);
                        NotifyData d = groupData.get(0);
                        notifyMessageArriving0(entry.getKey(), d.topic, d.queueId, maxOffset, groupData);
                    }
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                log.error("process pull notify error", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    protected void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        NotifyData d = new NotifyData(topic, queueId, maxOffset, null, 0, null, null);
        notifyMessageArriving0(buildKey(topic, queueId), topic, queueId, maxOffset, Collections.singletonList(d));
    }

    static class NotifyData {
        final String topic;
        final int queueId;
        final long maxOffset;
        final Long tagsCode;
        final long msgStoreTime;
        final byte[] filterBitMap;
        final Map<String, String> properties;
        NotifyData(String topic, int queueId, long maxOffset, Long tagsCode,
                long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
            this.topic = topic;
            this.queueId = queueId;
            this.maxOffset = maxOffset;
            this.tagsCode = tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.properties = properties;
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
            long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        try {
            notifyQueue.put(new NotifyData(topic, queueId, maxOffset, tagsCode, msgStoreTime, filterBitMap,
                    properties));
        } catch (InterruptedException e) {
            log.error("PullRequestHoldService get InterruptedException");
        }
    }

    private void notifyMessageArriving0(String key, String topic, int queueId, final long maxOffset,
            List<NotifyData> list) {
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = false;
                        for (NotifyData d : list) {
                            boolean match1 = request.getMessageFilter().isMatchedByConsumeQueue(d.tagsCode,
                                    new ConsumeQueueExt.CqExtUnit(d.tagsCode, d.msgStoreTime, d.filterBitMap));
                            // match by bit map, need eval again when properties is not null.
                            boolean match2 = d.properties == null || request.getMessageFilter()
                                    .isMatchedByCommitLog(null, d.properties);
                            if (match1 && match2) {
                                match = true;
                                break;
                            }
                        }

                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
