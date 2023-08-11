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

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;

public class CorrectLogicOffsetService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private long lastForceCorrectTime = -1L;

    private final DefaultMessageStore messageStore;

    public CorrectLogicOffsetService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void run() {
        try {
            this.correctLogicMinOffset();
        } catch (Throwable e) {
            LOGGER.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    private boolean needCorrect(ConsumeQueueInterface logic, long minPhyOffset, long lastForeCorrectTimeCurRun) {
        if (logic == null) {
            return false;
        }
        // If first exist and not available, it means first file may destroy failed, delete it.
        if (messageStore.getConsumeQueueStore().isFirstFileExist(logic) && !messageStore.getConsumeQueueStore().isFirstFileAvailable(logic)) {
            LOGGER.error("CorrectLogicOffsetService.needCorrect. first file not available, trigger correct." +
                    " topic:{}, queue:{}, maxPhyOffset in queue:{}, minPhyOffset " +
                    "in commit log:{}, minOffset in queue:{}, maxOffset in queue:{}, cqType:{}"
                , logic.getTopic(), logic.getQueueId(), logic.getMaxPhysicOffset()
                , minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getCQType());
            return true;
        }

        // logic.getMaxPhysicOffset() or minPhyOffset = -1
        // means there is no message in current queue, so no need to correct.
        if (logic.getMaxPhysicOffset() == -1 || minPhyOffset == -1) {
            return false;
        }

        if (logic.getMaxPhysicOffset() < minPhyOffset) {
            if (logic.getMinOffsetInQueue() < logic.getMaxOffsetInQueue()) {
                LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is less than min phy offset: {}, " +
                        "but min offset: {} is less than max offset: {}. topic:{}, queue:{}, cqType:{}."
                    , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                    , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                return true;
            } else if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                return false;
            } else {
                LOGGER.error("CorrectLogicOffsetService.needCorrect. It should not happen, logic max phy offset: {} is less than min phy offset: {}," +
                        " but min offset: {} is larger than max offset: {}. topic:{}, queue:{}, cqType:{}"
                    , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                    , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                return false;
            }
        }
        //the logic.getMaxPhysicOffset() >= minPhyOffset
        int forceCorrectInterval = messageStore.getMessageStoreConfig().getCorrectLogicMinOffsetForceInterval();
        if ((System.currentTimeMillis() - lastForeCorrectTimeCurRun) > forceCorrectInterval) {
            lastForceCorrectTime = System.currentTimeMillis();
            CqUnit cqUnit = logic.getEarliestUnit();
            if (cqUnit == null) {
                if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                    return false;
                } else {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. cqUnit is null, logic max phy offset: {} is greater than min phy offset: {}, " +
                            "but min offset: {} is not equal to max offset: {}. topic:{}, queue:{}, cqType:{}."
                        , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                        , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                }
            }

            if (cqUnit.getPos() < minPhyOffset) {
                LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is greater than min phy offset: {}, " +
                        "but minPhyPos in cq is: {}. min offset in queue: {}, max offset in queue: {}, topic:{}, queue:{}, cqType:{}."
                    , logic.getMaxPhysicOffset(), minPhyOffset, cqUnit.getPos(), logic.getMinOffsetInQueue()
                    , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                return true;
            }

            if (cqUnit.getPos() >= minPhyOffset) {

                // Normal case, do not need correct.
                return false;
            }
        }

        return false;
    }

    private void correctLogicMinOffset() {

        long lastForeCorrectTimeCurRun = lastForceCorrectTime;
        long minPhyOffset = messageStore.getMinPhyOffset();
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = messageStore.getConsumeQueueTable();
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (Objects.equals(CQType.SimpleCQ, logic.getCQType())) {
                    // cq is not supported for now.
                    continue;
                }
                if (needCorrect(logic, minPhyOffset, lastForeCorrectTimeCurRun)) {
                    doCorrect(logic, minPhyOffset);
                }
            }
        }
    }

    private void doCorrect(ConsumeQueueInterface logic, long minPhyOffset) {
        messageStore.getConsumeQueueStore().deleteExpiredFile(logic, minPhyOffset);
        int sleepIntervalWhenCorrectMinOffset = messageStore.getMessageStoreConfig().getCorrectLogicMinOffsetSleepInterval();
        if (sleepIntervalWhenCorrectMinOffset > 0) {
            try {
                Thread.sleep(sleepIntervalWhenCorrectMinOffset);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerConfig().getIdentifier() + CorrectLogicOffsetService.class.getSimpleName();
        }
        return CorrectLogicOffsetService.class.getSimpleName();
    }
}

