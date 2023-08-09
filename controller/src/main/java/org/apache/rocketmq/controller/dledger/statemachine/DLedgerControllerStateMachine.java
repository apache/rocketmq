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
package org.apache.rocketmq.controller.dledger.statemachine;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.ApplyEntry;
import io.openmessaging.storage.dledger.statemachine.ApplyEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.dledger.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventMessage;
import org.apache.rocketmq.controller.dledger.statemachine.event.write.WriteEventSerializer;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * The state machine implementation of the dledger controller
 */
public class DLedgerControllerStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ReplicasInfoManager replicasInfoManager;
    private final WriteEventSerializer eventSerializer;
    private final String dLedgerId;

    public DLedgerControllerStateMachine(final ReplicasInfoManager replicasInfoManager,
        final WriteEventSerializer eventSerializer, final DLedgerConfig dLedgerConfig) {
        this.replicasInfoManager = replicasInfoManager;
        this.eventSerializer = eventSerializer;
        this.dLedgerId = generateDLedgerId(dLedgerConfig.getGroup(), dLedgerConfig.getSelfId());
    }

    @Override
    public void onApply(ApplyEntryIterator iterator) {
        int applyingSize = 0;
        long firstApplyIndex = -1;
        long lastApplyIndex = -1;
        while (iterator.hasNext()) {
            final ApplyEntry applyEntry = iterator.next();
            final DLedgerEntry entry = applyEntry.getEntry();
            final byte[] body = entry.getBody();
            if (body != null && body.length > 0) {
                final WriteEventMessage event = this.eventSerializer.deserialize(body);
                EventResponse<?> result = this.replicasInfoManager.applyEvent(event);
                applyEntry.setResp(result);
            }
            firstApplyIndex = firstApplyIndex == -1 ? entry.getIndex() : firstApplyIndex;
            lastApplyIndex = entry.getIndex();
            applyingSize++;
        }
        log.info("Apply {} events index from {} to {} on controller {}", applyingSize, firstApplyIndex, lastApplyIndex, this.dLedgerId);
    }

    @Override
    public boolean onSnapshotSave(SnapshotWriter writer) {
        log.error("Controller {} snapshot save not supported", this.dLedgerId);
        return false;
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        log.error("Controller {} snapshot load not supported", this.dLedgerId);
        return false;
    }

    @Override
    public void onShutdown() {
    }

    @Override
    public void onError(DLedgerException e) {

    }

    @Override
    public String generateDLedgerId(String dLedgerGroupId, String dLedgerSelfId) {
        return StateMachine.super.generateDLedgerId(dLedgerGroupId, dLedgerSelfId);
    }

    @Override
    public String getBindDLedgerId() {
        return this.dLedgerId;
    }
}
