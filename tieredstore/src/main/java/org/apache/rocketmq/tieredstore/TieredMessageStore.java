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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.plugin.AbstractPluginMessageStore;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.tieredstore.core.MessageStoreDispatcher;
import org.apache.rocketmq.tieredstore.core.MessageStoreDispatcherImpl;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcher;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl;
import org.apache.rocketmq.tieredstore.core.MessageStoreFilter;
import org.apache.rocketmq.tieredstore.core.MessageStoreTopicFilter;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.index.IndexStoreService;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredMessageStore extends AbstractPluginMessageStore {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    protected static final long MIN_STORE_TIME = -1L;

    protected final String brokerName;
    protected final MessageStore defaultStore;
    protected final MessageStoreConfig storeConfig;
    protected final MessageStorePluginContext context;

    protected final MetadataStore metadataStore;
    protected final MessageStoreExecutor storeExecutor;
    protected final IndexService indexService;
    protected final FlatFileStore flatFileStore;
    protected final MessageStoreFilter topicFilter;
    protected final MessageStoreFetcher fetcher;
    protected final MessageStoreDispatcher dispatcher;

    public TieredMessageStore(MessageStorePluginContext context, MessageStore next) {
        super(context, next);

        this.storeConfig = new MessageStoreConfig();
        this.context = context;
        this.context.registerConfiguration(this.storeConfig);
        this.brokerName = this.storeConfig.getBrokerName();
        this.defaultStore = next;

        this.metadataStore = this.getMetadataStore(this.storeConfig);
        this.topicFilter = new MessageStoreTopicFilter(this.storeConfig);
        this.storeExecutor = new MessageStoreExecutor();
        this.flatFileStore = new FlatFileStore(this.storeConfig, this.metadataStore, this.storeExecutor);
        this.indexService = new IndexStoreService(this.flatFileStore.getFlatFileFactory(),
            MessageStoreUtil.getIndexFilePath(this.storeConfig.getBrokerName()));
        this.fetcher = new MessageStoreFetcherImpl(this);
        this.dispatcher = new MessageStoreDispatcherImpl(this);
        next.addDispatcher(dispatcher);
    }

    @Override
    public boolean load() {
        boolean loadFlatFile = flatFileStore.load();
        boolean loadNextStore = next.load();
        boolean result = loadFlatFile && loadNextStore;
        if (result) {
            indexService.start();
            dispatcher.start();
            storeExecutor.commonExecutor.scheduleWithFixedDelay(
                flatFileStore::scheduleDeleteExpireFile, storeConfig.getTieredStoreDeleteFileInterval(),
                storeConfig.getTieredStoreDeleteFileInterval(), TimeUnit.MILLISECONDS);
        }
        return result;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public MessageStore getDefaultStore() {
        return defaultStore;
    }

    private MetadataStore getMetadataStore(MessageStoreConfig storeConfig) {
        try {
            Class<? extends MetadataStore> clazz =
                Class.forName(storeConfig.getTieredMetadataServiceProvider()).asSubclass(MetadataStore.class);
            Constructor<? extends MetadataStore> constructor = clazz.getConstructor(MessageStoreConfig.class);
            return constructor.newInstance(storeConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public MessageStoreFilter getTopicFilter() {
        return topicFilter;
    }

    public MessageStoreExecutor getStoreExecutor() {
        return storeExecutor;
    }

    public FlatFileStore getFlatFileStore() {
        return flatFileStore;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public boolean fetchFromCurrentStore(String topic, int queueId, long offset) {
        return fetchFromCurrentStore(topic, queueId, offset, 1);
    }

    @SuppressWarnings("all")
    public boolean fetchFromCurrentStore(String topic, int queueId, long offset, int batchSize) {
        MessageStoreConfig.TieredStorageLevel storageLevel = storeConfig.getTieredStorageLevel();

        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.FORCE)) {
            return true;
        }

        if (!storageLevel.isEnable()) {
            return false;
        }

        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return false;
        }

        if (offset >= flatFile.getConsumeQueueCommitOffset()) {
            return false;
        }

        // determine whether tiered storage path conditions are met
        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.NOT_IN_DISK)) {
            // return true to read from tiered storage if the CommitLog is empty
            if (next != null && next.getCommitLog() != null &&
                next.getCommitLog().getMinOffset() < 0L) {
                return true;
            }
            if (!next.checkInStoreByConsumeOffset(topic, queueId, offset)) {
                return true;
            }
        }

        if (storageLevel.check(MessageStoreConfig.TieredStorageLevel.NOT_IN_MEM)
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

        // for system topic, force reading from local store
        if (topicFilter.filterTopic(topic)) {
            return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
        }

        if (fetchFromCurrentStore(topic, queueId, offset, maxMsgNums)) {
            log.trace("GetMessageAsync from remote store, " +
                "topic: {}, queue: {}, offset: {}, maxCount: {}", topic, queueId, offset, maxMsgNums);
        } else {
            log.trace("GetMessageAsync from next store, " +
                "topic: {}, queue: {}, offset: {}, maxCount: {}", topic, queueId, offset, maxMsgNums);
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
                        log.debug("GetMessageAsync not found, then back to next store, result: {}, " +
                                "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                            result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                        return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
                    }
                }

                if (result.getStatus() != GetMessageStatus.FOUND &&
                    result.getStatus() != GetMessageStatus.NO_MESSAGE_IN_QUEUE &&
                    result.getStatus() != GetMessageStatus.NO_MATCHED_LOGIC_QUEUE &&
                    result.getStatus() != GetMessageStatus.OFFSET_TOO_SMALL &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_ONE &&
                    result.getStatus() != GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    log.warn("GetMessageAsync not found and message is not in next store, result: {}, " +
                            "topic: {}, queue: {}, queue offset: {}, offset range: {}-{}",
                        result.getStatus(), topic, queueId, offset, result.getMinOffset(), result.getMaxOffset());
                }

                if (result.getStatus() == GetMessageStatus.FOUND) {
                    Attributes messagesOutAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                        .build();
                    TieredStoreMetricsManager.messagesOutTotal.add(result.getMessageCount(), messagesOutAttributes);

                    if (next.getStoreStatsService() != null) {
                        next.getStoreStatsService().getGetMessageTransferredMsgCount().add(result.getMessageCount());
                    }
                }

                // Fix min or max offset according next store at last
                long minOffsetInQueue = next.getMinOffsetInQueue(topic, queueId);
                if (minOffsetInQueue >= 0 && minOffsetInQueue < result.getMinOffset()) {
                    result.setMinOffset(minOffsetInQueue);
                }

                // In general, the local cq offset is slightly greater than the commit offset in read message,
                // so there is no need to update the maximum offset to the local cq offset here,
                // otherwise it will cause repeated consumption after next start offset over commit offset.

                if (storeConfig.isRecordGetMessageResult()) {
                    log.info("GetMessageAsync result, {}, group: {}, topic: {}, queueId: {}, offset: {}, count:{}",
                        result, group, topic, queueId, offset, maxMsgNums);
                }

                return result;
            }).exceptionally(e -> {
                log.error("GetMessageAsync from tiered store failed", e);
                return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
            });
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        long minOffsetInNextStore = next.getMinOffsetInQueue(topic, queueId);
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
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
        long localMinTime = next.getEarliestMessageTime(topic, queueId);
        return fetcher.getEarliestMessageTimeAsync(topic, queueId)
            .thenApply(remoteMinTime -> {
                if (localMinTime > MIN_STORE_TIME && remoteMinTime > MIN_STORE_TIME) {
                    return Math.min(localMinTime, remoteMinTime);
                }
                return localMinTime > MIN_STORE_TIME ? localMinTime :
                    (remoteMinTime > MIN_STORE_TIME ? remoteMinTime : MIN_STORE_TIME);
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
                        .put(TieredStoreMetricsConstant.LABEL_OPERATION,
                                TieredStoreMetricsConstant.OPERATION_API_GET_TIME_BY_OFFSET)
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                        .build();
                    TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
                    if (time == -1) {
                        log.debug("GetEarliestMessageTimeAsync failed, try to get message time from next store, topic: {}, queue: {}, queue offset: {}",
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
        boolean isForce = storeConfig.getTieredStorageLevel() == MessageStoreConfig.TieredStorageLevel.FORCE;
        if (timestamp < next.getEarliestMessageTime() || isForce) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            long offsetInTieredStore = fetcher.getOffsetInQueueByTime(topic, queueId, timestamp, boundaryType);
            Attributes latencyAttributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_OPERATION, TieredStoreMetricsConstant.OPERATION_API_GET_OFFSET_BY_TIME)
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                .build();
            TieredStoreMetricsManager.apiLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), latencyAttributes);
            if (offsetInTieredStore == -1L && !isForce) {
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
            log.warn("TieredMessageStore#queryMessageAsync: get earliest message time in next store failed: {}", earliestTimeInNextStore);
        }
        boolean isForce = storeConfig.getTieredStorageLevel() == MessageStoreConfig.TieredStorageLevel.FORCE;
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
                log.error("TieredMessageStore#queryMessageAsync: query message in tiered store failed", e);
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
        TieredStoreMetricsManager.init(meter, attributesBuilderSupplier, storeConfig, fetcher, flatFileStore, next);
    }

    @Override
    public int cleanUnusedTopic(Set<String> retainTopics) {
        metadataStore.iterateTopic(topicMetadata -> {
            String topic = topicMetadata.getTopic();
            if (retainTopics.contains(topic) ||
                TopicValidator.isSystemTopic(topic) ||
                MixAll.isLmq(topic)) {
                return;
            }
            this.deleteTopics(Sets.newHashSet(topicMetadata.getTopic()));
        });
        return next.cleanUnusedTopic(retainTopics);
    }

    @Override
    public int deleteTopics(Set<String> deleteTopics) {
        for (String topic : deleteTopics) {
            metadataStore.iterateQueue(topic, queueMetadata -> {
                flatFileStore.destroyFile(queueMetadata.getQueue());
            });
            metadataStore.deleteTopic(topic);
            log.info("MessageStore delete topic success, topicName={}", topic);
        }
        return next.deleteTopics(deleteTopics);
    }

    @Override
    public synchronized void shutdown() {
        if (next != null) {
            next.shutdown();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
        if (indexService != null) {
            indexService.shutdown();
        }
        if (flatFileStore != null) {
            flatFileStore.shutdown();
        }
        if (storeExecutor != null) {
            storeExecutor.shutdown();
        }
    }

    @Override
    public void destroy() {
        if (next != null) {
            next.destroy();
        }
        if (indexService != null) {
            indexService.destroy();
        }
        if (flatFileStore != null) {
            flatFileStore.destroy();
        }
        if (metadataStore != null) {
            metadataStore.destroy();
        }
    }
}
