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
package org.apache.rocketmq.store.service;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.index.QueryOffsetResult;

public class QueryMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore messageStore;

    public QueryMessageService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = messageStore.getIndexService().queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    MessageExt msg = messageStore.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    SelectMappedBufferResult result = messageStore.getCommitLog().getData(offset, false);
                    if (result != null) {
                        int size = result.getByteBuffer().getInt(0);
                        result.getByteBuffer().limit(size);
                        result.setSize(size);
                        queryMessageResult.addMessage(result);
                    }
                } catch (Exception e) {
                    LOGGER.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key,
        int maxNum, long begin, long end) {
        return CompletableFuture.completedFuture(queryMessage(topic, key, maxNum, begin, end));
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = messageStore.getCommitLog().getMessage(commitLogOffset, size);
        if (null == sbr) {
            return null;
        }

        try {
            return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
        } finally {
            sbr.release();
        }
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = messageStore.getCommitLog().getMessage(commitLogOffset, 4);
        if (null == sbr) {
            return null;
        }

        try {
            // 1 TOTALSIZE
            int size = sbr.getByteBuffer().getInt();
            return lookMessageByOffset(commitLogOffset, size);
        } finally {
            sbr.release();
        }
    }

    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = messageStore.getCommitLog().getMessage(commitLogOffset, 4);
        if (null == sbr) {
            return null;

        }

        try {
            // 1 TOTALSIZE
            int size = sbr.getByteBuffer().getInt();
            return messageStore.getCommitLog().getMessage(commitLogOffset, size);
        } finally {
            sbr.release();
        }
    }

    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return messageStore.getCommitLog().getMessage(commitLogOffset, msgSize);
    }


}
