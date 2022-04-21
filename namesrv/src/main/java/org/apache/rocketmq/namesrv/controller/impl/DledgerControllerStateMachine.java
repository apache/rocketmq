package org.apache.rocketmq.namesrv.controller.impl;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.controller.manager.ReplicasInfoManager;
import org.apache.rocketmq.namesrv.controller.manager.event.EventMessage;
import org.apache.rocketmq.namesrv.controller.manager.event.EventSerializer;

/**
 * The state machine implementation of the dledger controller
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/18 20:38
 */
public class DledgerControllerStateMachine implements StateMachine {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ReplicasInfoManager replicasInfoManager;
    private final EventSerializer eventSerializer;
    private final String dledgerId;

    public DledgerControllerStateMachine(final ReplicasInfoManager replicasInfoManager,
        final EventSerializer eventSerializer, final String dledgerId) {
        this.replicasInfoManager = replicasInfoManager;
        this.eventSerializer = eventSerializer;
        this.dledgerId = dledgerId;
    }

    @Override
    public void onApply(CommittedEntryIterator iterator) {
        final List<EventMessage> events = new ArrayList<>();
        while (iterator.hasNext()) {
            final DLedgerEntry entry = iterator.next();
            final byte[] body = entry.getBody();
            if (body != null && body.length > 0) {
                final EventMessage event = this.eventSerializer.deserialize(body);
                if (event != null) {
                    events.add(event);
                }
            }
        }
        log.info("Apply {} events on controller {}", events.size(), this.dledgerId);
        for (EventMessage event : events) {
            this.replicasInfoManager.applyEvent(event);
        }
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
}
