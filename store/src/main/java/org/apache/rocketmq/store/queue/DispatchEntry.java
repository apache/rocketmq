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

package org.apache.rocketmq.store.queue;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.rocketmq.store.DispatchRequest;

/**
 * Use Record when Java 16 is available
 */
public class DispatchEntry {
    public byte[] topic;
    public int queueId;
    public long queueOffset;
    public long commitLogOffset;
    public int messageSize;
    public long tagCode;
    public long storeTimestamp;

    public static DispatchEntry from(@Nonnull DispatchRequest request) {
        DispatchEntry entry = new DispatchEntry();
        entry.topic = request.getTopic().getBytes(StandardCharsets.UTF_8);
        entry.queueId = request.getQueueId();
        entry.queueOffset = request.getConsumeQueueOffset();
        entry.commitLogOffset = request.getCommitLogOffset();
        entry.messageSize = request.getMsgSize();
        entry.tagCode = request.getTagsCode();
        entry.storeTimestamp = request.getStoreTimestamp();
        return entry;
    }
}
