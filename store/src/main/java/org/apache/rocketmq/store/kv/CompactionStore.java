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
package org.apache.rocketmq.store.kv;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionStore {

    public static final String COMPACTION_DIR = "compaction";
    public static final String COMPACTION_LOG_DIR = "compactionLog";
    public static final String COMPACTION_CQ_DIR = "compactionCq";

    private final String compactionPath;
    private final String compactionLogPath;
    private final String compactionCqPath;
    private final MessageStore defaultMessageStore;
    private final CompactionPositionMgr positionMgr;
    private final ConcurrentHashMap<String, CompactionLog> compactionLogTable;
    private final ScheduledExecutorService compactionSchedule;
    private final int compactionInterval;
    private final int compactionThreadNum;
    private final int offsetMapSize;
    private String masterAddr;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public CompactionStore(MessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.compactionLogTable = new ConcurrentHashMap<>();
        MessageStoreConfig config = defaultMessageStore.getMessageStoreConfig();
        String storeRootPath = config.getStorePathRootDir();
        this.compactionPath = Paths.get(storeRootPath, COMPACTION_DIR).toString();
        this.compactionLogPath = Paths.get(compactionPath, COMPACTION_LOG_DIR).toString();
        this.compactionCqPath = Paths.get(compactionPath, COMPACTION_CQ_DIR).toString();
        this.positionMgr = new CompactionPositionMgr(compactionPath);
        if (config.getCompactionThreadNum() <= 0) {
            this.compactionThreadNum = Runtime.getRuntime().availableProcessors();
        } else {
            this.compactionThreadNum = config.getCompactionThreadNum();
        }
        this.compactionSchedule = Executors.newScheduledThreadPool(this.compactionThreadNum,
            new ThreadFactoryImpl("compactionSchedule_"));
        this.offsetMapSize = config.getMaxOffsetMapSize() / compactionThreadNum;

        this.compactionInterval = defaultMessageStore.getMessageStoreConfig().getCompactionScheduleInternal();
    }

    public void load(boolean exitOk) throws Exception {
        File logRoot = new File(compactionLogPath);
        File[] fileTopicList = logRoot.listFiles();
        if (fileTopicList != null) {
            for (File fileTopic : fileTopicList) {
                if (!fileTopic.isDirectory()) {
                    continue;
                }

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        if (!fileQueueId.isDirectory()) {
                            continue;
                        }
                        try {
                            String topic = fileTopic.getName();
                            int queueId = Integer.parseInt(fileQueueId.getName());

                            if (Files.isDirectory(Paths.get(compactionCqPath, topic, String.valueOf(queueId)))) {
                                CompactionLog log = new CompactionLog(defaultMessageStore, this, topic, queueId);
                                log.load(exitOk);
                                compactionLogTable.put(topic + "_" + queueId, log);
                                compactionSchedule.scheduleWithFixedDelay(log::doCompaction, compactionInterval, compactionInterval, TimeUnit.MILLISECONDS);
                            } else {
                                log.error("{}:{} compactionLog mismatch with compactionCq", topic, queueId);
                            }
                        } catch (Exception e) {
                            log.error("load compactionLog {}:{} exception: ",
                                fileTopic.getName(), fileQueueId.getName(), e);
                            throw new Exception("load compactionLog " + fileTopic.getName()
                                + ":" + fileQueueId.getName() + " exception: " + e.getMessage());
                        }
                    }
                }
            }
        }
        log.info("compactionStore {}:{} load completed.", compactionLogPath, compactionCqPath);
    }

    public void putMessage(String topic, int queueId, SelectMappedBufferResult smr) throws Exception {
        CompactionLog clog = compactionLogTable.compute(topic + "_" + queueId, (k, v) -> {
            if (v == null) {
                try {
                    v = new CompactionLog(defaultMessageStore,this, topic, queueId);
                    v.load(true);
                    compactionSchedule.scheduleWithFixedDelay(v::doCompaction, compactionInterval, compactionInterval, TimeUnit.MILLISECONDS);
                } catch (IOException e) {
                    log.error("create compactionLog exception: ", e);
                    return null;
                }
            }
            return v;
        });

        if (clog != null) {
            clog.asyncPutMessage(smr);
        }
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize) {
        CompactionLog log = compactionLogTable.get(topic + "_" + queueId);
        if (log == null) {
            return GetMessageResult.NO_MATCH_LOGIC_QUEUE;
        } else {
            return log.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        }

    }

    public void flushCQ(int flushLeastPages) {
        compactionLogTable.values().forEach(log -> log.flushCQ(flushLeastPages));
    }

    public void updateMasterAddress(String addr) {
        this.masterAddr = addr;
    }

    public void shutdown() {
        positionMgr.persist();
        compactionSchedule.shutdown();
        try {
            if (!compactionSchedule.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                List<Runnable> droppedTasks = compactionSchedule.shutdownNow();
                log.warn("compactionSchedule was abruptly shutdown. {} tasks will not be executed.", droppedTasks.size());
            }
        } catch (InterruptedException e) {
            log.warn("wait compaction schedule shutdown interrupted. ");
        }
    }

    public ScheduledExecutorService getCompactionSchedule() {
        return compactionSchedule;
    }

    public String getCompactionLogPath() {
        return compactionLogPath;
    }

    public String getCompactionCqPath() {
        return compactionCqPath;
    }

    public CompactionPositionMgr getPositionMgr() {
        return positionMgr;
    }

    public int getOffsetMapSize() {
        return offsetMapSize;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

}
