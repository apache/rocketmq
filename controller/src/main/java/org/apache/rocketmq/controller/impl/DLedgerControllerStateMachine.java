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
package org.apache.rocketmq.controller.impl;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventSerializer;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * The state machine implementation of the dledger controller
 */
public class DLedgerControllerStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ReplicasInfoManager replicasInfoManager;
    private final EventSerializer eventSerializer;
    private final String dLedgerId;

    public DLedgerControllerStateMachine(final ReplicasInfoManager replicasInfoManager,
        final EventSerializer eventSerializer, final String dLedgerGroupId, final String dLedgerSelfId) {
        this.replicasInfoManager = replicasInfoManager;
        this.eventSerializer = eventSerializer;
        this.dLedgerId = generateDLedgerId(dLedgerGroupId, dLedgerSelfId);
    }

    @Override
    public void onApply(CommittedEntryIterator iterator) {
        int applyingSize = 0;
        long firstApplyIndex = -1;
        long lastApplyIndex = -1;
        while (iterator.hasNext()) {
            final DLedgerEntry entry = iterator.next();
            final byte[] body = entry.getBody();
            if (body != null && body.length > 0) {
                final EventMessage event = this.eventSerializer.deserialize(body);
                this.replicasInfoManager.applyEvent(event);
            }
            firstApplyIndex = firstApplyIndex == -1 ? entry.getIndex() : firstApplyIndex;
            lastApplyIndex = entry.getIndex();
            applyingSize++;
        }
        log.info("Apply {} events index from {} to {} on controller {}", applyingSize, firstApplyIndex, lastApplyIndex, this.dLedgerId);
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, CompletableFuture<Boolean> future) {
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return false;
    }

    @Override
    public void onShutdown() {
    }

    @Override
    public String getBindDLedgerId() {
        return this.dLedgerId;
    }
}
