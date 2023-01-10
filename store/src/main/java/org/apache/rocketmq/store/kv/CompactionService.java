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

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CompactionService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final CompactionStore compactionStore;
    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;
    private final LinkedBlockingQueue<TopicPartitionOffset> compactionMsgQ = new LinkedBlockingQueue<>();

    public CompactionService(CommitLog commitLog, DefaultMessageStore messageStore, CompactionStore compactionStore) {
        this.commitLog = commitLog;
        this.defaultMessageStore = messageStore;
        this.compactionStore = compactionStore;
    }

    public void putRequest(DispatchRequest request) {
        if (request == null) {
            return;
        }

        String topic = request.getTopic();
        Optional<TopicConfig> topicConfig = defaultMessageStore.getTopicConfig(topic);
        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(topicConfig);
        //check request topic flag
        if (Objects.equals(policy, CleanupPolicy.COMPACTION)) {
            int queueId = request.getQueueId();
            long physicalOffset = request.getCommitLogOffset();
            TopicPartitionOffset tpo = new TopicPartitionOffset(topic, queueId, physicalOffset);
            compactionMsgQ.offer(tpo);
            this.wakeup();
        } // else skip if message isn't compaction
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final int maxTotalMsgSize) {
        return compactionStore.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerConfig().getIdentifier() + CompactionService.class.getSimpleName();
        }
        return CompactionService.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!isStopped()) {
            try {
                TopicPartitionOffset tpo = compactionMsgQ.poll(1, TimeUnit.MILLISECONDS);
                if (null != tpo) {
                    SelectMappedBufferResult smr = null;
                    try {
                        smr = commitLog.getData(tpo.physicalOffset);
                        if (smr != null) {
                            compactionStore.putMessage(tpo.topic, tpo.queueId, smr);
                        }
                    } catch (Exception e) {
                        log.error("putMessage into {}:{} compactionLog exception: ", tpo.topic, tpo.queueId, e);
                    } finally {
                        if (smr != null) {
                            smr.release();
                        }
                    }
                } else {
                    waitForRunning(100);
                }
            } catch (InterruptedException e) {
                log.error("poll from compaction pos queue interrupted.");
            }
        }
    }

    public boolean load(boolean exitOK) {
        try {
            compactionStore.load(exitOK);
            return true;
        } catch (Exception e) {
            log.error("load compaction store error ", e);
            return false;
        }
    }

//    @Override
//    public void start() {
//        compactionStore.load();
//        super.start();
//    }

    @Override
    public void shutdown() {
        super.shutdown();
        compactionStore.shutdown();
    }

    public void updateMasterAddress(String addr) {
        compactionStore.updateMasterAddress(addr);
    }

    static class TopicPartitionOffset {
        String topic;
        int queueId;
        long physicalOffset;

        public TopicPartitionOffset(final String topic, final int queueId, final long physicalOffset) {
            this.topic = topic;
            this.queueId = queueId;
            this.physicalOffset = physicalOffset;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }

        public long getPhysicalOffset() {
            return physicalOffset;
        }

        public void setPhysicalOffset(long physicalOffset) {
            this.physicalOffset = physicalOffset;
        }
    }

}
