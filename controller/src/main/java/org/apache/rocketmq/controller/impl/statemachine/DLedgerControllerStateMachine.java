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
package org.apache.rocketmq.controller.impl.statemachine;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventSerializer;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * The state machine implementation of the DLedger controller
 */
public class DLedgerControllerStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ReplicasInfoManager replicasInfoManager;
    private final EventSerializer eventSerializer;
    private final String dLedgerId;
    private final StatemachineSnapshotFileGenerator snapshotFileGenerator;

    public DLedgerControllerStateMachine(final ReplicasInfoManager replicasInfoManager,
                                         final EventSerializer eventSerializer, final String dLedgerId) {
        this.replicasInfoManager = replicasInfoManager;
        this.eventSerializer = eventSerializer;
        this.dLedgerId = dLedgerId;
        this.snapshotFileGenerator = new StatemachineSnapshotFileGenerator(Collections.singletonList(replicasInfoManager));
    }

    @Override
    public void onApply(CommittedEntryIterator iterator) {
        int applyingSize = 0;
        while (iterator.hasNext()) {
            final DLedgerEntry entry = iterator.next();
            final byte[] body = entry.getBody();
            if (body != null && body.length > 0) {
                final EventMessage event = this.eventSerializer.deserialize(body);
                this.replicasInfoManager.applyEvent(event);
            }
            applyingSize++;
        }
        log.info("Apply {} events on controller {}", applyingSize, this.dLedgerId);
    }

    @Override
    public boolean onSnapshotSave(SnapshotWriter writer) {
        final String snapshotStorePath = writer.getSnapshotStorePath();
        try {
            this.snapshotFileGenerator.generateSnapshot(snapshotStorePath);
            return true;
        } catch (IOException e) {
            log.error("Failed to generate controller statemachine snapshot", e);
            return false;
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            return this.snapshotFileGenerator.loadSnapshot(reader.getSnapshotStorePath());
        } catch (IOException e) {
            log.error("Failed to load controller statemachine snapshot", e);
            return false;
        }
    }


    @Override
    public void onShutdown() {
        log.info("Controller statemachine shutdown!");
    }

    @Override
    public void onError(DLedgerException e) {
        log.error("Error happen in controller statemachine", e);
    }

    @Override
    public String getBindDLedgerId() {
        return dLedgerId;
    }
}
