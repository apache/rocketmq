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

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.util.Objects;
import java.util.Optional;

public class CompactionService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final CompactionStore compactionStore;
    private final DefaultMessageStore defaultMessageStore;
    private final CommitLog commitLog;

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
            SelectMappedBufferResult smr = null;
            try {
                smr = commitLog.getData(request.getCommitLogOffset());
                if (smr != null) {
                    compactionStore.doDispatch(request, smr);
                }
            } catch (Exception e) {
                log.error("putMessage into {}:{} compactionLog exception: ", request.getTopic(), request.getQueueId(), e);
            } finally {
                if (smr != null) {
                    smr.release();
                }
            }
        } // else skip if message isn't compaction
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

    public void shutdown() {
        compactionStore.shutdown();
    }

    public void updateMasterAddress(String addr) {
        compactionStore.updateMasterAddress(addr);
    }
}
