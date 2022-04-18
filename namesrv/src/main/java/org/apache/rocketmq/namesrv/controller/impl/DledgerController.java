package org.apache.rocketmq.namesrv.controller.impl;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.controller.Controller;
import org.apache.rocketmq.namesrv.controller.manager.ReplicasInfoManager;
import org.apache.rocketmq.namesrv.controller.manager.event.ControllerResult;
import org.apache.rocketmq.remoting.common.ServiceThread;

/**
 * The implementation of controllerApi, based on dledger (raft).
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 14:58
 */
public class DledgerController extends ServiceThread implements Controller {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final DLedgerServer dLedgerServer;
    private final DLedgerConfig dLedgerConfig;
    private final ReplicasInfoManager replicasInfoManager;
    private final DledgerControllerStateMachine statemachine;
    private final BlockingQueue<ControllerEvent> eventQueue;
    public DledgerController(final NamesrvConfig namesrvConfig) {
        this.dLedgerConfig = new DLedgerConfig();
        dLedgerConfig.setEnableDiskForceClean(true);
        dLedgerConfig.setStoreType(DLedgerConfig.FILE);
        dLedgerConfig.setGroup(namesrvConfig.getControllerDLegerGroup());
        dLedgerConfig.setPeers(namesrvConfig.getControllerDLegerPeers());
        dLedgerConfig.setSelfId(namesrvConfig.getControllerDLegerSelfId());
        dLedgerConfig.setStoreBaseDir(namesrvConfig.getControllerStrorePath());

        this.replicasInfoManager = new ReplicasInfoManager(namesrvConfig.isEnableElectUncleanMaster());
        this.statemachine = new DledgerControllerStateMachine(replicasInfoManager);

        this.dLedgerServer = new DLedgerServer(this.dLedgerConfig);
        this.dLedgerServer.registerStateMachine(this.statemachine);

        this.eventQueue = new LinkedBlockingQueue<>(1024);
    }

    @Override
    public String getServiceName() {
        return DledgerController.class.getName();
    }

    @Override
    public void run() {
        while (!isStopped()) {
            try {
                final ControllerEvent event = this.eventQueue.poll(5, TimeUnit.SECONDS);
                if (event != null) {
                    event.run();
                }
            } catch (final InterruptedException e) {
                log.error("Error happen in {} when pull event from event queue", getServiceName(), e);
            }
        }
    }

    private <T> CompletableFuture<T> appendEvent(final String name, final Supplier<ControllerResult<T>> supplier,
        boolean isWriteEvent) {
        final ControllerEvent<T> event = isWriteEvent ? new ControllerWriteEvent<>(name, supplier) :
            new ControllerReadEvent<>(name, supplier);
        int tryTimes = 0;
        while (true) {
            try {
                if (!this.eventQueue.offer(event, 10, TimeUnit.SECONDS)) {
                    continue;
                }
                return event.future();
            } catch (final InterruptedException e) {
                log.error("Error happen in {} when append read event", getServiceName(), e);
                tryTimes++;
                if (tryTimes > 3) {
                    event.future().cancel(true);
                    return event.future();
                }
            }
        }
    }

    @Override
    public CompletableFuture<AlterInSyncReplicasResponseHeader> alterInSyncReplicas(
        AlterInSyncReplicasRequestHeader request) {
        return appendEvent("alterInSyncReplicas",
            () -> this.replicasInfoManager.alterSyncStateSet(request), true);
    }

    @Override
    public CompletableFuture<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request) {
        return appendEvent("electMaster",
            () -> this.replicasInfoManager.electMaster(request), true);
    }

    @Override
    public CompletableFuture<RegisterBrokerResponseHeader> registerBroker(RegisterBrokerRequestHeader request) {
        return appendEvent("registerBroker",
            () -> this.replicasInfoManager.registerBroker(request), true);
    }

    @Override
    public CompletableFuture<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        return appendEvent("getReplicaInfo",
            () -> this.replicasInfoManager.getReplicaInfo(request), false);
    }

    @Override
    public CompletableFuture<GetMetaDataResponseHeader> getMetadata(final GetMetaDataRequestHeader request) {
        return null;
    }

    interface ControllerEvent<T> {
        /**
         * Run the controller event
         */
        void run();

        /**
         * Return the completableFuture
         */
        CompletableFuture<T> future();
    }

    static class ControllerReadEvent<T> implements ControllerEvent<T> {
        private final String name;
        private final Supplier<ControllerResult<T>> supplier;
        private final CompletableFuture<T> future;

        ControllerReadEvent(String name, Supplier<ControllerResult<T>> supplier) {
            this.name = name;
            this.supplier = supplier;
            this.future = new CompletableFuture<>();
        }

        @Override
        public void run() {

        }

        @Override
        public CompletableFuture<T> future() {
            return this.future;
        }
    }

    static class ControllerWriteEvent<T> implements ControllerEvent<T> {
        private final String name;
        private final Supplier<ControllerResult<T>> supplier;
        private final CompletableFuture<T> future;

        ControllerWriteEvent(String name, Supplier<ControllerResult<T>> supplier) {
            this.name = name;
            this.supplier = supplier;
            this.future = new CompletableFuture<>();
        }

        @Override
        public void run() {

        }

        @Override
        public CompletableFuture<T> future() {
            return this.future;
        }
    }
}
