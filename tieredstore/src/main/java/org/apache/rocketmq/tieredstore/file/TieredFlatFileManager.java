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
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredFlatFileManager {

    private static final Logger BROKER_LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static volatile TieredFlatFileManager instance;
    private static volatile TieredIndexFile indexFile;

    private final TieredMetadataStore metadataStore;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFileAllocator tieredFileAllocator;
    private final ConcurrentMap<MessageQueue, CompositeQueueFlatFile> flatFileConcurrentMap;

    public TieredFlatFileManager(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.flatFileConcurrentMap = new ConcurrentHashMap<>();
        this.doScheduleTask();
    }

    public static TieredFlatFileManager getInstance(TieredMessageStoreConfig storeConfig) {
        if (storeConfig == null || instance != null) {
            return instance;
        }
        synchronized (TieredFlatFileManager.class) {
            if (instance == null) {
                try {
                    instance = new TieredFlatFileManager(storeConfig);
                } catch (Exception e) {
                    logger.error("Construct FlatFileManager instance error", e);
                }
            }
        }
        return instance;
    }

    public static TieredIndexFile getIndexFile(TieredMessageStoreConfig storeConfig) {
        if (storeConfig == null) {
            return indexFile;
        }

        if (indexFile == null) {
            synchronized (TieredFlatFileManager.class) {
                if (indexFile == null) {
                    try {
                        String filePath = TieredStoreUtil.toPath(new MessageQueue(
                            TieredStoreUtil.RMQ_SYS_TIERED_STORE_INDEX_TOPIC, storeConfig.getBrokerName(), 0));
                        indexFile = new TieredIndexFile(new TieredFileAllocator(storeConfig), filePath);
                    } catch (Exception e) {
                        logger.error("Construct FlatFileManager indexFile error", e);
                    }
                }
            }
        }
        return indexFile;
    }

    public void doCommit() {
        Random random = new Random();
        for (CompositeQueueFlatFile flatFile : deepCopyFlatFileToList()) {
            int delay = random.nextInt(storeConfig.getMaxCommitJitter());
            TieredStoreExecutor.commitExecutor.schedule(() -> {
                try {
                    flatFile.commitCommitLog();
                } catch (Throwable e) {
                    MessageQueue mq = flatFile.getMessageQueue();
                    logger.error("Commit commitLog periodically failed: topic: {}, queue: {}",
                        mq.getTopic(), mq.getQueueId(), e);
                }
            }, delay, TimeUnit.MILLISECONDS);
            TieredStoreExecutor.commitExecutor.schedule(() -> {
                try {
                    flatFile.commitConsumeQueue();
                } catch (Throwable e) {
                    MessageQueue mq = flatFile.getMessageQueue();
                    logger.error("Commit consumeQueue periodically failed: topic: {}, queue: {}",
                        mq.getTopic(), mq.getQueueId(), e);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
        TieredStoreExecutor.commitExecutor.schedule(() -> {
            try {
                if (indexFile != null) {
                    indexFile.commit(true);
                }
            } catch (Throwable e) {
                logger.error("Commit indexFile periodically failed", e);
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    public void doCleanExpiredFile() {
        long expiredTimeStamp = System.currentTimeMillis() -
            TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());
        for (CompositeQueueFlatFile flatFile : deepCopyFlatFileToList()) {
            TieredStoreExecutor.cleanExpiredFileExecutor.submit(() -> {
                flatFile.getCompositeFlatFileLock().lock();
                try {
                    flatFile.cleanExpiredFile(expiredTimeStamp);
                    flatFile.destroyExpiredFile();
                    if (flatFile.getConsumeQueueBaseOffset() == -1) {
                        logger.info("Clean flatFile because file not initialized, topic={}, queueId={}",
                            flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId());
                        destroyCompositeFile(flatFile.getMessageQueue());
                    }
                } finally {
                    flatFile.getCompositeFlatFileLock().unlock();
                }
            });
        }
        if (indexFile != null) {
            indexFile.cleanExpiredFile(expiredTimeStamp);
            indexFile.destroyExpiredFile();
        }
    }

    private void doScheduleTask() {
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                doCommit();
            } catch (Throwable e) {
                logger.error("Commit flat file periodically failed: ", e);
            }
        }, 60, 60, TimeUnit.SECONDS);

        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                doCleanExpiredFile();
            } catch (Throwable e) {
                logger.error("Clean expired flat file failed: ", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public boolean load() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            flatFileConcurrentMap.clear();
            this.recoverSequenceNumber();
            this.recoverTieredFlatFile();
            logger.info("Message store recover end, total cost={}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            long costTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            logger.info("Message store recover error, total cost={}ms", costTime);
            BROKER_LOG.error("Message store recover error, total cost={}ms", costTime, e);
            return false;
        }
        return true;
    }

    public void recoverSequenceNumber() {
        AtomicLong topicSequenceNumber = new AtomicLong();
        metadataStore.iterateTopic(topicMetadata -> {
            if (topicMetadata != null && topicMetadata.getTopicId() > 0) {
                topicSequenceNumber.set(Math.max(topicSequenceNumber.get(), topicMetadata.getTopicId()));
            }
        });
        metadataStore.setTopicSequenceNumber(topicSequenceNumber.incrementAndGet());
    }

    public void recoverTieredFlatFile() {
        Semaphore semaphore = new Semaphore((int) (TieredStoreExecutor.QUEUE_CAPACITY * 0.75));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        metadataStore.iterateTopic(topicMetadata -> {
            try {
                semaphore.acquire();
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        Stopwatch subWatch = Stopwatch.createStarted();
                        if (topicMetadata.getStatus() != 0) {
                            return;
                        }
                        AtomicLong queueCount = new AtomicLong();
                        metadataStore.iterateQueue(topicMetadata.getTopic(), queueMetadata -> {
                            this.getOrCreateFlatFileIfAbsent(new MessageQueue(topicMetadata.getTopic(),
                                storeConfig.getBrokerName(), queueMetadata.getQueue().getQueueId()));
                            queueCount.incrementAndGet();
                        });

                        if (queueCount.get() == 0L) {
                            metadataStore.deleteTopic(topicMetadata.getTopic());
                        } else {
                            logger.info("Recover TopicFlatFile, topic: {}, queueCount: {}, cost: {}ms",
                                topicMetadata.getTopic(), queueCount.get(), subWatch.elapsed(TimeUnit.MILLISECONDS));
                        }
                    } catch (Exception e) {
                        logger.error("Recover TopicFlatFile error, topic: {}", topicMetadata.getTopic(), e);
                    } finally {
                        semaphore.release();
                    }
                }, TieredStoreExecutor.commitExecutor);
                futures.add(future);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public void cleanup() {
        flatFileConcurrentMap.clear();
        cleanStaticReference();
    }

    private static void cleanStaticReference() {
        instance = null;
        indexFile = null;
    }

    @Nullable
    public CompositeQueueFlatFile getOrCreateFlatFileIfAbsent(MessageQueue messageQueue) {
        return flatFileConcurrentMap.computeIfAbsent(messageQueue, mq -> {
            try {
                logger.debug("Create new TopicFlatFile, topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId());
                return new CompositeQueueFlatFile(tieredFileAllocator, mq);
            } catch (Exception e) {
                logger.debug("Create new TopicFlatFile failed, topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId(), e);
            }
            return null;
        });
    }

    public CompositeQueueFlatFile getFlatFile(MessageQueue messageQueue) {
        return flatFileConcurrentMap.get(messageQueue);
    }

    public ImmutableList<CompositeQueueFlatFile> deepCopyFlatFileToList() {
        return ImmutableList.copyOf(flatFileConcurrentMap.values());
    }

    public void shutdown() {
        if (indexFile != null) {
            indexFile.commit(true);
        }
        for (CompositeFlatFile flatFile : deepCopyFlatFileToList()) {
            flatFile.shutdown();
        }
    }

    public void destroy() {
        if (indexFile != null) {
            indexFile.destroy();
        }
        ImmutableList<CompositeQueueFlatFile> flatFileList = deepCopyFlatFileToList();
        cleanup();
        for (CompositeFlatFile flatFile : flatFileList) {
            flatFile.destroy();
        }
    }

    public void destroyCompositeFile(MessageQueue mq) {
        if (mq == null) {
            return;
        }

        // delete memory reference
        CompositeQueueFlatFile flatFile = flatFileConcurrentMap.remove(mq);
        if (flatFile != null) {
            MessageQueue messageQueue = flatFile.getMessageQueue();
            logger.info("TieredFlatFileManager#destroyCompositeFile: " +
                    "try to destroy composite flat file: topic: {}, queueId: {}",
                messageQueue.getTopic(), messageQueue.getQueueId());

            // delete queue metadata
            flatFile.destroy();
        }
    }
}
