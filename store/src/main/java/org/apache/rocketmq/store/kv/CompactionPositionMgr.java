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

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionPositionMgr extends ConfigManager {

    public static final String CHECKPOINT_FILE = "position-checkpoint";

    private transient String compactionPath;
    private transient String checkpointFileName;

    private ConcurrentHashMap<String, Long> queueOffsetMap = new ConcurrentHashMap<>();

    private CompactionPositionMgr() {

    }

    public CompactionPositionMgr(final String compactionPath) {
        this.compactionPath = compactionPath;
        this.checkpointFileName = compactionPath + File.separator + CHECKPOINT_FILE;
        this.load();
    }

    public void setOffset(String topic, int queueId, final long offset) {
        queueOffsetMap.put(topic + "_" + queueId, offset);
    }

    public long getOffset(String topic, int queueId) {
        return queueOffsetMap.getOrDefault(topic + "_" + queueId, -1L);
    }

    public boolean isEmpty() {
        return queueOffsetMap.isEmpty();
    }

    public boolean isCompaction(String topic, int queueId, long offset) {
        return getOffset(topic, queueId) > offset;
    }

    @Override
    public String configFilePath() {
        return checkpointFileName;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            CompactionPositionMgr obj = RemotingSerializable.fromJson(jsonString, CompactionPositionMgr.class);
            if (obj != null) {
                this.queueOffsetMap = obj.queueOffsetMap;
            }
        }
    }

    public ConcurrentHashMap<String, Long> getQueueOffsetMap() {
        return queueOffsetMap;
    }

    public void setQueueOffsetMap(ConcurrentHashMap<String, Long> queueOffsetMap) {
        this.queueOffsetMap = queueOffsetMap;
    }
}
