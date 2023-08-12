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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.hook.PutMessageHook;

public class PutMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore messageStore;

    private final List<PutMessageHook> putMessageHookList = new ArrayList<>();

    public PutMessageService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(msg);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
            && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOGGER.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = messageStore.getTopicConfig(msg.getTopic());
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOGGER.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
            }
        }

        return addPutCallback(msg);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(messageExtBatch);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        return addPutCallback(messageExtBatch);
    }

    private CompletableFuture<PutMessageResult> addPutCallback(MessageExtBatch messageExtBatch) {
        long beginTime = messageStore.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = messageStore.getCommitLog().asyncPutMessages(messageExtBatch);

        putResultFuture.thenAccept(result -> {
            long eclipseTime = messageStore.getSystemClock().now() - beginTime;
            if (eclipseTime > 500) {
                LOGGER.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
            }
            messageStore.getStoreStatsService().setPutMessageEntireTimeMax(eclipseTime);

            if (null == result || !result.isOk()) {
                messageStore.getStoreStatsService().getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    private CompletableFuture<PutMessageResult> addPutCallback(MessageExtBrokerInner msg) {
        long beginTime = messageStore.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = messageStore.getCommitLog().asyncPutMessage(msg);
        putResultFuture.thenAccept(result -> {
            long elapsedTime = messageStore.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                LOGGER.warn("DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms, topic={}, bodyLength={}",
                    elapsedTime, msg.getTopic(), msg.getBody().length);
            }
            messageStore.getStoreStatsService().setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                messageStore.getStoreStatsService().getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return waitForPutResult(asyncPutMessage(msg));
    }

    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return waitForPutResult(asyncPutMessages(messageExtBatch));
    }

    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                Math.max(messageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                    messageStore.getMessageStoreConfig().getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            LOGGER.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    public List<PutMessageHook> getPutMessageHookList() {
        return putMessageHookList;
    }

}
