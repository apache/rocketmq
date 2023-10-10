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
package org.apache.rocketmq.tieredstore;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.plugin.AbstractPluginMessageStore;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.file.CompositeFlatFile;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.rocksdb.RocksDBException;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;

public class TieredMessageStore extends AbstractPluginMessageStore {

    protected static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String brokerName;
    protected final TieredMessageStoreConfig storeConfig;
    protected final TieredMetadataStore metadataStore;

    protected final TieredDispatcher dispatcher;
    protected final TieredMessageFetcher fetcher;
    protected final TieredFlatFileManager flatFileManager;

    public TieredMessageStore(MessageStorePluginContext context, MessageStore next) {
        super(context, next);
        this.storeConfig = new TieredMessageStoreConfig();
        context.registerConfiguration(storeConfig);
        this.brokerName = storeConfig.getBrokerName();
        TieredStoreUtil.addSystemTopic(storeConfig.getBrokerClusterName());
        TieredStoreUtil.addSystemTopic(brokerName);

        TieredStoreExecutor.init();
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        this.fetcher = new TieredMessageFetcher(storeConfig);
        this.dispatcher = new TieredDispatcher(next, storeConfig);

        this.flatFileManager = TieredFlatFileManager.getInstance(storeConfig);
        next.addDispatcher(dispatcher);
    }

    @Override
    public boolean load() {
        boolean loadFlatFile = flatFileManager.load();
        boolean loadNextStore = next.load();
        boolean result = loadFlatFile && loadNextStore;
        if (result) {
            dispatcher.initScheduleTask();
            dispatcher.start();
        }
        return result;
    }

    public TieredMessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public boolean fetchFromCurrentStore(String topic, int queueId, long offset) {
        return fetchFromCurrentStore(topic, queueId, offset, 1);
    }

    public boolean fetchFromCurrentStore(String topic, int queueId, long offset, int batchSize) {
        TieredMessageStoreConfig.TieredStorageLevel deepStorageLevel = storeConfig.getTieredStorageLevel();

        if (deepStorageLevel.check(TieredMessageStoreConfig.TieredStorageLevel.FORCE)) {
            return true;
        }

        if (!deepStorageLevel.isEnable()) {
            return false;
        }

        CompositeFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return false;
        }

        if (offset >= flatFile.getConsumeQueueCommitOffset()) {
            return false;
        }

        // determine whether tiered storage path conditions are met
        if (deepStorageLevel.check(TieredMessageStoreConfig.TieredStorageLevel.NOT_IN_DISK)
            && !next.checkInStoreByConsumeOffset(topic, queueId, offset)) {
            return true;
        }

        if (deepStorageLevel.check(TieredMessageStoreConfig.TieredStorageLevel.NOT_IN_MEM)
            && !next.checkInMemByConsumeOffset(topic, queueId, offset, batchSize)) {
            return true;
        }
        return false;
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums,
        MessageFilter messageFilter) {
        return getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter).join();
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {

        // For system topic, force reading from local store
        if (TieredStoreUtil.isSystemTopic(topic) || PopAckConstants.isStartWithRevivePrefix(topic)) {
            return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
        }

        if (fetchFromCurrentStore(topic, queueId, offset, maxMsgNums)) {
            logger.trace("GetMessageAsync from current store, topic: {}, queue: {}, offset: {}", topic, queueId, offset);
        } else {
            logger.trace("GetMessageAsync from next store, topic: {}, queue: {}, offset: {}", topic, queueId, offset);
            return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        return fetcher
            .getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter)
            .thenApply(result -> {

                Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_MESSAGE)
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                    .build();
                TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);

                if (result.getStatus() == GetMessageStatus.OFFSET_FOUND_NULL ||
                    result.getStatus() == GetMessageStatus.NO_MATCHED_LOGIC_QUEUE) {

                    if (next.checkInStoreByConsumeOffset(topic, queueId, offset)) {
                        TieredStoreMetricsManager.fallbackTotal.add(1, latencyAttributes);
                        logger.debug("GetMessageAsync not found, then back to next store, result: {}, " +
                                "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                            result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                        return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
                    }
                }

                if (result.getStatus() != GetMessageStatus.FOUND &&
                    result.getStatus() != GetMessageStatus.NO_MATCHED_LOGIC_QUEUE &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_ONE &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    logger.warn("GetMessageAsync not found and message is not in next store, result: {}, " +
                            "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                        result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                }

                if (result.getStatus() == GetMessageStatus.FOUND) {
                    Attributes messagesOutAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                        .build();
                    TieredStoreMetricsManager.messagesOutTotal.add(result.getMessageCount(), messagesOutAttributes);
                }

                // Fix min or max offset according next store at last
                long minOffsetInQueue = next.getMinOffsetInQueue(topic, queueId);
                if (minOffsetInQueue >= 0 && minOffsetInQueue < result.getMinOffset()) {
                    result.setMinOffset(minOffsetInQueue);
                }

                // In general, the local cq offset is slightly greater than the commit offset in read message,
                // so there is no need to update the maximum offset to the local cq offset here,
                // otherwise it will cause repeated consumption after next begin offset over commit offset.

                logger.trace("GetMessageAsync result, group: {}, topic: {}, queueId: {}, offset: {}, count:{}, {}",
                    group, topic, queueId, offset, maxMsgNums, result);

                return result;
            }).exceptionally(e -> {
                logger.error("GetMessageAsync from tiered store failed", e);
                return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
            });
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        return super.asyncPutMessage(msg);
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        long minOffsetInNextStore = next.getMinOffsetInQueue(topic, queueId);
        CompositeFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return minOffsetInNextStore;
        }
        long minOffsetInTieredStore = flatFile.getConsumeQueueMinOffset();
        if (minOffsetInTieredStore < 0) {
            return minOffsetInNextStore;
        }
        return Math.min(minOffsetInNextStore, minOffsetInTieredStore);
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return getEarliestMessageTimeAsync(topic, queueId).join();
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        long nextEarliestMessageTime = next.getEarliestMessageTime(topic, queueId);
        long finalNextEarliestMessageTime = nextEarliestMessageTime > 0 ? nextEarliestMessageTime : Long.MAX_VALUE;
        Stopwatch stopwatch = Stopwatch.createStarted();
        return fetcher.getEarliestMessageTimeAsync(topic, queueId)
            .thenApply(time -> {
                Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_EARLIEST_MESSAGE_TIME)
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .build();
                TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                if (time < 0) {
                    logger.debug("GetEarliestMessageTimeAsync failed, try to get earliest message time from next store: topic: {}, queue: {}",
                        topic, queueId);
                    return finalNextEarliestMessageTime != Long.MAX_VALUE ? finalNextEarliestMessageTime : -1;
                }
                return Math.min(finalNextEarliestMessageTime, time);
            });
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId,
        long consumeQueueOffset) {
        if (fetchFromCurrentStore(topic, queueId, consumeQueueOffset)) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            return fetcher.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset)
                .thenApply(time -> {
                    Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_TIME_BY_OFFSET)
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .build();
                    TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                    if (time == -1) {
                        logger.debug("GetEarliestMessageTimeAsync failed, try to get message time from next store, topic: {}, queue: {}, queue offset: {}",
                            topic, queueId, consumeQueueOffset);
                        return next.getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset);
                    }
                    return time;
                });
        }
        return next.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return getOffsetInQueueByTime(topic, queueId, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        long earliestTimeInNextStore = next.getEarliestMessageTime();
        if (earliestTimeInNextStore <= 0) {
            logger.warn("TieredMessageStore#getOffsetInQueueByTimeAsync: get earliest message time in next store failed: {}", earliestTimeInNextStore);
            return next.getOffsetInQueueByTime(topic, queueId, timestamp);
        }
        boolean isForce = storeConfig.getTieredStorageLevel() == TieredMessageStoreConfig.TieredStorageLevel.FORCE;
        if (timestamp < earliestTimeInNextStore || isForce) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long offsetInTieredStore = fetcher.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
            Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_OFFSET_BY_TIME)
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                .build();
            TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
            if (offsetInTieredStore == -1 && !isForce) {
                return next.getOffsetInQueueByTime(topic, queueId, timestamp);
            }
            return offsetInTieredStore;
        }
        return next.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return queryMessageAsync(topic, key, maxNum, begin, end).join();
    }

    @Override
    public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key,
        int maxNum, long begin, long end) {
        long earliestTimeInNextStore = next.getEarliestMessageTime();
        if (earliestTimeInNextStore <= 0) {
            logger.warn("TieredMessageStore#queryMessageAsync: get earliest message time in next store failed: {}", earliestTimeInNextStore);
        }
        boolean isForce = storeConfig.getTieredStorageLevel() == TieredMessageStoreConfig.TieredStorageLevel.FORCE;
        QueryMessageResult result = end < earliestTimeInNextStore || isForce ?
            new QueryMessageResult() :
            next.queryMessage(topic, key, maxNum, begin, end);
        int resultSize = result.getMessageBufferList().size();
        if (resultSize < maxNum && begin < earliestTimeInNextStore || isForce) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return fetcher.queryMessageAsync(topic, key, maxNum - resultSize, begin, isForce ? end : earliestTimeInNextStore)
                    .thenApply(tieredStoreResult -> {
                        Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                            .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_QUERY_MESSAGE)
                            .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                            .build();
                        TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                        for (SelectMappedBufferResult msg : tieredStoreResult.getMessageMapedList()) {
                            result.addMessage(msg);
                        }
                        return result;
                    });
            } catch (Exception e) {
                logger.error("TieredMessageStore#queryMessageAsync: query message in tiered store failed", e);
                return CompletableFuture.completedFuture(result);
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Pair<InstrumentSelector, ViewBuilder>> res = super.getMetricsView();
        res.addAll(TieredStoreMetricsManager.getMetricsView());
        return res;
    }

    @Override
    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        super.initMetrics(meter, attributesBuilderSupplier);
        TieredStoreMetricsManager.init(meter, attributesBuilderSupplier, storeConfig, fetcher, next);
    }

    @Override
    public void shutdown() {
        next.shutdown();

        dispatcher.shutdown();
        TieredFlatFileManager.getInstance(storeConfig).shutdown();
        TieredStoreExecutor.shutdown();
    }

    @Override
    public void destroy() {
        next.destroy();

        TieredFlatFileManager.getInstance(storeConfig).destroy();
        try {
            metadataStore.destroy();
        } catch (Exception e) {
            logger.error("TieredMessageStore#destroy: destroy metadata store failed", e);
        }
    }

    @Override
    public int cleanUnusedTopic(Set<String> retainTopics) throws RocksDBException {
        try {
            metadataStore.iterateTopic(topicMetadata -> {
                String topic = topicMetadata.getTopic();
                if (retainTopics.contains(topic) ||
                    TopicValidator.isSystemTopic(topic) ||
                    MixAll.isLmq(topic)) {
                    return;
                }
                this.destroyCompositeFlatFile(topicMetadata.getTopic());
            });
        } catch (Exception e) {
            logger.error("TieredMessageStore#cleanUnusedTopic: iterate topic metadata failed", e);
        }
        return next.cleanUnusedTopic(retainTopics);
    }

    @Override
    public int deleteTopics(Set<String> deleteTopics) throws RocksDBException {
        for (String topic : deleteTopics) {
            this.destroyCompositeFlatFile(topic);
        }
        return next.deleteTopics(deleteTopics);
    }

    public void destroyCompositeFlatFile(String topic) {
        try {
            if (StringUtils.isBlank(topic)) {
                return;
            }
            metadataStore.iterateQueue(topic, queueMetadata -> {
                flatFileManager.destroyCompositeFile(queueMetadata.getQueue());
            });
            // delete topic metadata
            metadataStore.deleteTopic(topic);
            logger.info("Destroy composite flat file in message store, topic={}", topic);
        } catch (Exception e) {
            logger.error("Destroy composite flat file in message store failed, topic={}", topic, e);
        }
    }
}
