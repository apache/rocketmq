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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredFlatFileManager {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static volatile TieredFlatFileManager instance;
    private static volatile TieredIndexFile indexFile;

    private final TieredMetadataStore metadataStore;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFileAllocator tieredFileAllocator;
    private final ConcurrentMap<MessageQueue, CompositeQueueFlatFile> queueFlatFileMap;

    public TieredFlatFileManager(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.queueFlatFileMap = new ConcurrentHashMap<>();
        this.doScheduleTask();
    }

    public static TieredFlatFileManager getInstance(TieredMessageStoreConfig storeConfig) {
        if (storeConfig == null) {
            return instance;
        }

        if (instance == null) {
            synchronized (TieredFlatFileManager.class) {
                if (instance == null) {
                    try {
                        instance = new TieredFlatFileManager(storeConfig);
                    } catch (Exception e) {
                        logger.error("TieredFlatFileManager#getInstance: create flat file manager failed", e);
                    }
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
                        logger.error("TieredFlatFileManager#getIndexFile: create index file failed", e);
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
                    logger.error("commit commitLog periodically failed: topic: {}, queue: {}",
                        mq.getTopic(), mq.getQueueId(), e);
                }
            }, delay, TimeUnit.MILLISECONDS);
            TieredStoreExecutor.commitExecutor.schedule(() -> {
                try {
                    flatFile.commitConsumeQueue();
                } catch (Throwable e) {
                    MessageQueue mq = flatFile.getMessageQueue();
                    logger.error("commit consumeQueue periodically failed: topic: {}, queue: {}",
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
                logger.error("commit indexFile periodically failed", e);
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    public void doCleanExpiredFile() {
        long expiredTimeStamp = System.currentTimeMillis() -
            TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());
        Random random = new Random();
        for (CompositeQueueFlatFile flatFile : deepCopyFlatFileToList()) {
            int delay = random.nextInt(storeConfig.getMaxCommitJitter());
            TieredStoreExecutor.cleanExpiredFileExecutor.schedule(() -> {
                flatFile.getCompositeFlatFileLock().lock();
                try {
                    flatFile.cleanExpiredFile(expiredTimeStamp);
                    flatFile.destroyExpiredFile();
                    if (flatFile.getConsumeQueueBaseOffset() == -1) {
                        destroyCompositeFile(flatFile.getMessageQueue());
                    }
                } finally {
                    flatFile.getCompositeFlatFileLock().unlock();
                }
            }, delay, TimeUnit.MILLISECONDS);
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
                logger.error("commit flat file periodically failed: ", e);
            }
        }, 60, 60, TimeUnit.SECONDS);

        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                doCleanExpiredFile();
            } catch (Throwable e) {
                logger.error("clean expired flat file failed: ", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public boolean load() {
        try {
            AtomicLong topicSequenceNumber = new AtomicLong();
            List<Future<?>> futureList = new ArrayList<>();
            queueFlatFileMap.clear();
            metadataStore.iterateTopic(topicMetadata -> {
                topicSequenceNumber.set(Math.max(topicSequenceNumber.get(), topicMetadata.getTopicId()));
                Future<?> future = TieredStoreExecutor.dispatchExecutor.submit(() -> {
                    if (topicMetadata.getStatus() != 0) {
                        return;
                    }
                    try {
                        metadataStore.iterateQueue(topicMetadata.getTopic(),
                            queueMetadata -> getOrCreateFlatFileIfAbsent(
                                new MessageQueue(topicMetadata.getTopic(),
                                    storeConfig.getBrokerName(),
                                    queueMetadata.getQueue().getQueueId())));
                    } catch (Exception e) {
                        logger.error("load mq composite flat file from metadata failed", e);
                    }
                });
                futureList.add(future);
            });

            // Wait for load all metadata done
            for (Future<?> future : futureList) {
                future.get();
            }
            metadataStore.setTopicSequenceNumber(topicSequenceNumber.incrementAndGet());
        } catch (Exception e) {
            logger.error("load mq composite flat file from metadata failed", e);
            return false;
        }
        return true;
    }

    public void cleanup() {
        queueFlatFileMap.clear();
        cleanStaticReference();
    }

    private static void cleanStaticReference() {
        instance = null;
        indexFile = null;
    }

    @Nullable
    public CompositeQueueFlatFile getOrCreateFlatFileIfAbsent(MessageQueue messageQueue) {
        return queueFlatFileMap.computeIfAbsent(messageQueue, mq -> {
            try {
                logger.info("TieredFlatFileManager#getOrCreateFlatFileIfAbsent: " +
                        "try to create new flat file: topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId());
                return new CompositeQueueFlatFile(tieredFileAllocator, mq);
            } catch (Exception e) {
                logger.error("TieredFlatFileManager#getOrCreateFlatFileIfAbsent: " +
                        "create new flat file: topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId(), e);
                return null;
            }
        });
    }

    public CompositeQueueFlatFile getFlatFile(MessageQueue messageQueue) {
        return queueFlatFileMap.get(messageQueue);
    }

    public ImmutableList<CompositeQueueFlatFile> deepCopyFlatFileToList() {
        return ImmutableList.copyOf(queueFlatFileMap.values());
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
        CompositeQueueFlatFile flatFile = queueFlatFileMap.remove(mq);
        if (flatFile != null) {
            MessageQueue messageQueue = flatFile.getMessageQueue();
            logger.info("TieredFlatFileManager#destroyCompositeFile: " +
                    "try to destroy composite flat file: topic: {}, queueId: {}",
                messageQueue.getTopic(), messageQueue.getQueueId());
            flatFile.destroy();
        }
    }
}
