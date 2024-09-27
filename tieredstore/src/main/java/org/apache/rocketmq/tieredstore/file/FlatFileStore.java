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
package org.apache.rocketmq.tieredstore.file;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileStore {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final MetadataStore metadataStore;
    private final MessageStoreConfig storeConfig;
    private final MessageStoreExecutor executor;
    private final FlatFileFactory flatFileFactory;
    private final ConcurrentMap<MessageQueue, FlatMessageFile> flatFileConcurrentMap;

    public FlatFileStore(MessageStoreConfig storeConfig, MetadataStore metadataStore, MessageStoreExecutor executor) {
        this.storeConfig = storeConfig;
        this.metadataStore = metadataStore;
        this.executor = executor;
        this.flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);
        this.flatFileConcurrentMap = new ConcurrentHashMap<>();
    }

    public boolean load() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            this.flatFileConcurrentMap.clear();
            this.recover();
            log.info("FlatFileStore recover finished, total cost={}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            long costTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            log.info("FlatFileStore recover error, total cost={}ms", costTime);
            LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME)
                .error("FlatFileStore recover error, total cost={}ms", costTime, e);
            return false;
        }
        return true;
    }

    public void recover() {
        Semaphore semaphore = new Semaphore(storeConfig.getTieredStoreMaxPendingLimit() / 4);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        metadataStore.iterateTopic(topicMetadata -> {
            semaphore.acquireUninterruptibly();
            futures.add(this.recoverAsync(topicMetadata)
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        log.error("FlatFileStore recover file error, topic={}", topicMetadata.getTopic(), throwable);
                    }
                    semaphore.release();
                }));
        });
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public CompletableFuture<Void> recoverAsync(TopicMetadata topicMetadata) {
        return CompletableFuture.runAsync(() -> {
            Stopwatch stopwatch = Stopwatch.createStarted();
            AtomicLong queueCount = new AtomicLong();
            metadataStore.iterateQueue(topicMetadata.getTopic(), queueMetadata -> {
                FlatMessageFile flatFile = this.computeIfAbsent(new MessageQueue(
                    topicMetadata.getTopic(), storeConfig.getBrokerName(), queueMetadata.getQueue().getQueueId()));
                queueCount.incrementAndGet();
                log.debug("FlatFileStore recover file, topicId={}, topic={}, queueId={}, cost={}ms",
                    flatFile.getTopicId(), flatFile.getMessageQueue().getTopic(),
                    flatFile.getMessageQueue().getQueueId(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
            });
            log.info("FlatFileStore recover file, topic={}, total={}, cost={}ms",
                topicMetadata.getTopic(), queueCount.get(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }, executor.bufferCommitExecutor);
    }

    public void scheduleDeleteExpireFile() {
        if (!storeConfig.isTieredStoreDeleteFileEnable()) {
            return;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        ImmutableList<FlatMessageFile> fileList = this.deepCopyFlatFileToList();
        for (FlatMessageFile flatFile : fileList) {
            flatFile.getFileLock().lock();
            try {
                flatFile.destroyExpiredFile(System.currentTimeMillis() -
                    TimeUnit.HOURS.toMillis(flatFile.getFileReservedHours()));
            } catch (Exception e) {
                log.error("FlatFileStore delete expire file error", e);
            } finally {
                flatFile.getFileLock().unlock();
            }
        }
        log.info("FlatFileStore schedule delete expired file, count={}, cost={}ms",
            fileList.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public FlatFileFactory getFlatFileFactory() {
        return flatFileFactory;
    }

    public FlatMessageFile computeIfAbsent(MessageQueue messageQueue) {
        return flatFileConcurrentMap.computeIfAbsent(messageQueue,
            mq -> new FlatMessageFile(flatFileFactory, mq.getTopic(), mq.getQueueId()));
    }

    public FlatMessageFile getFlatFile(MessageQueue messageQueue) {
        return flatFileConcurrentMap.get(messageQueue);
    }

    public ImmutableList<FlatMessageFile> deepCopyFlatFileToList() {
        return ImmutableList.copyOf(flatFileConcurrentMap.values());
    }

    public void shutdown() {
        flatFileConcurrentMap.values().forEach(FlatMessageFile::shutdown);
    }

    public void destroyFile(MessageQueue mq) {
        if (mq == null) {
            return;
        }

        FlatMessageFile flatFile = flatFileConcurrentMap.remove(mq);
        if (flatFile != null) {
            flatFile.shutdown();
            flatFile.destroy();
        }
        log.info("FlatFileStore destroy file, topic={}, queueId={}", mq.getTopic(), mq.getQueueId());
    }

    public void destroy() {
        this.shutdown();
        flatFileConcurrentMap.values().forEach(FlatMessageFile::destroy);
        flatFileConcurrentMap.clear();
    }
}
