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

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MessageStoreStateMachine {
    protected final Logger log;

    private MessageStoreState currentState;
    private long lastStateChangeTimestamp;
    private final long startTimestamp;

    public enum MessageStoreState {
        INIT(0),

        LOAD_BEGIN(10),
        LOAD_COMMITLOG_OK(11),
        LOAD_CONSUME_QUEUE_OK(12),
        LOAD_COMPACTION_OK(13),
        LOAD_INDEX_OK(14),

        RECOVER_BEGIN(20),
        RECOVER_CONSUME_QUEUE_OK(21),
        RECOVER_COMMITLOG_OK(22),
        RECOVER_TOPIC_QUEUE_TABLE_OK(23),

        RUNNING(30),

        SHUTDOWN_BEGIN(40),
        SHUTDOWN_OK(41);

        final int order;

        MessageStoreState(int order) {
            this.order = order;
        }

        public int getOrder() {
            return order;
        }

        public boolean isBefore(MessageStoreState storeState) {
            return this.order < storeState.order;
        }

        public boolean isAfter(MessageStoreState storeState) {
            return this.order > storeState.order;
        }
    }


    public MessageStoreStateMachine(Logger log) {
        this.log = log == null ? LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME) : log;
        this.currentState = MessageStoreState.INIT;
        this.startTimestamp = System.currentTimeMillis();
        this.lastStateChangeTimestamp = startTimestamp;
        logStateChange(null, currentState, true);
    }

    public void transitTo(MessageStoreState newState) {
        transitTo(newState, true);
    }

    public void transitTo(MessageStoreState newState, boolean success) {
        if (!newState.isAfter(currentState)) {
            throw new IllegalStateException(
                String.format("Invalid state transition from %s to %s. Can only move forward.",
                    currentState, newState)
            );
        }

        logStateChange(currentState, newState, success);
        if (success) {
            this.currentState = newState;
            this.lastStateChangeTimestamp = System.currentTimeMillis();
        }
    }

    private void logStateChange(MessageStoreState fromState, MessageStoreState toState, boolean success) {
        if (fromState == null && success) {
            log.info("MessageStoreState initialized, state={}", toState);
        } else if (success) {
            log.info("MessageStoreState transition from {} to {}; Time in previous state={}ms, Total time={}ms",
                fromState, toState, getCurrentStateRunningTimeMs(), getTotalRunningTimeMs());
        } else {
            log.warn("MessageStoreState transition from {} to {} failed; Time in previous state={}ms, Total "
                + "time={}ms", fromState, toState, getCurrentStateRunningTimeMs(), getTotalRunningTimeMs());
        }
    }

    public MessageStoreState getCurrentState() {
        return currentState;
    }

    public long getTotalRunningTimeMs() {
        return System.currentTimeMillis() - startTimestamp;
    }

    public long getCurrentStateRunningTimeMs() {
        return System.currentTimeMillis() - lastStateChangeTimestamp;
    }
}
