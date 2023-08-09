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
package org.apache.rocketmq.broker.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerScheduleService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final int HA_ADDRESS_MIN_LENGTH = 6;

    private long lastSyncTimeMs = System.currentTimeMillis();



    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService;


    private ScheduledExecutorService syncBrokerMemberGroupExecutorService;
    private ScheduledExecutorService brokerHeartbeatExecutorService;

    public BrokerScheduleService(
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig,
        final BrokerController brokerController
    ) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerController = brokerController;

        initPoolExecutors();
    }

    public void init() {
        initializeBrokerScheduledTasks();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().getBrokerOuterAPI().refreshMetadata();
                } catch (Exception e) {
                    LOG.error("ScheduledTask refresh metadata exception", e);
                }
            }
        }, 10, 5, TimeUnit.SECONDS);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            updateNamesrvAddr();
            LOG.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        updateNamesrvAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to update nameServer address list", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        getBrokerController().getBrokerOuterAPI().fetchNameServerAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to fetch nameServer address", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFetchNamesrvAddrInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void start() {
        scheduleRegisterBrokerAll();
        scheduleSlaveActingMaster();

        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }
    }

    public void shutdown() {
        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        shutdownScheduledExecutorService(this.scheduledExecutorService);
        shutdownScheduledExecutorService(this.syncBrokerMemberGroupExecutorService);
        shutdownScheduledExecutorService(this.brokerHeartbeatExecutorService);
    }

    public void sendHeartbeat() {
        if (this.brokerConfig.isEnableControllerMode()) {
            getBrokerController().getReplicasManager().sendHeartbeatToController();
        }

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            if (this.brokerConfig.isCompatibleWithOldNameSrv()) {
                getBrokerController().getBrokerOuterAPI().sendHeartbeatViaDataVersion(
                    this.brokerConfig.getBrokerClusterName(),
                    getBrokerController().getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    getBrokerController().getTopicConfigManager().getDataVersion(),
                    this.brokerConfig.isInBrokerContainer());
            } else {
                getBrokerController().getBrokerOuterAPI().sendHeartbeat(
                    this.brokerConfig.getBrokerClusterName(),
                    getBrokerController().getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.brokerConfig.isInBrokerContainer());
            }
        }
    }

    public void shutdownScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService == null) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            LOG.warn("shutdown ScheduledExecutorService was Interrupted!  ", ignore);
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleRegisterBrokerAll() {
        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    if (System.currentTimeMillis() < getBrokerController().getShouldStartTime()) {
                        LOG.info("Register to namesrv after {}", getBrokerController().getShouldStartTime());
                        return;
                    }
                    if (getBrokerController().isIsolated()) {
                        BrokerScheduleService.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    getBrokerController().getBrokerServiceRegistry().registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));
    }

    public void scheduleSlaveActingMaster() {
        if (!this.brokerConfig.isEnableSlaveActingMaster()) {
            return;
        }

        scheduleSendHeartbeat();

        scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    getBrokerController().syncBrokerMemberGroup();
                } catch (Throwable e) {
                    LOG.error("sync BrokerMemberGroup error. ", e);
                }
            }
        }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
    }

    public void scheduleSendHeartbeat() {
        scheduledFutures.add(this.brokerHeartbeatExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(getBrokerController().getBrokerIdentity()) {
            @Override
            public void run0() {
                if (getBrokerController().isIsolated()) {
                    return;
                }
                try {
                    sendHeartbeat();
                } catch (Exception e) {
                    LOG.error("sendHeartbeat Exception", e);
                }

            }
        }, 1000, brokerConfig.getBrokerHeartbeatInterval(), TimeUnit.MILLISECONDS));
    }

    public void initializeBrokerScheduledTasks() {
        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = TimeUnit.DAYS.toMillis(1);
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().getBrokerStats().record();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to record broker stats", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().getConsumerOffsetManager().persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().getConsumerFilterManager().persist();
                    getBrokerController().getConsumerOrderInfoManager().persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerFilter or consumerOrderInfo",
                        e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().protectBroker();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to protectBroker", e);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    getBrokerController().printWaterMark();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to print broker watermark", e);
                }
            }
        }, 10, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Dispatch task fall behind commit log {}bytes",
                        getBrokerController().getMessageStore().dispatchBehindBytes());
                } catch (Throwable e) {
                    LOG.error("Failed to print dispatchBehindBytes", e);
                }
            }
        }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

        if (!messageStoreConfig.isEnableDLegerCommitLog() && !messageStoreConfig.isDuplicationEnable() && !brokerConfig.isEnableControllerMode()) {
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= HA_ADDRESS_MIN_LENGTH) {
                    this.getBrokerController().getMessageStore().updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    getBrokerController().setUpdateMasterHAServerAddrPeriodically(false);
                } else {
                    getBrokerController().setUpdateMasterHAServerAddrPeriodically(true);
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            if (System.currentTimeMillis() - lastSyncTimeMs > 60 * 1000) {
                                getBrokerController().getSlaveSynchronize().syncAll();
                                lastSyncTimeMs = System.currentTimeMillis();
                            }

                            //timer checkpoint, latency-sensitive, so sync it more frequently
                            if (messageStoreConfig.isTimerWheelEnable()) {
                                getBrokerController().getSlaveSynchronize().syncTimerCheckPoint();
                            }
                        } catch (Throwable e) {
                            LOG.error("Failed to sync all config for slave.", e);
                        }
                    }
                }, 1000 * 10, 3 * 1000, TimeUnit.MILLISECONDS);

            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            LOG.error("Failed to print diff of master and slave.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            getBrokerController().setUpdateMasterHAServerAddrPeriodically(true);
        }
    }

    public void updateNamesrvAddr() {
        if (this.brokerConfig.isFetchNameSrvAddrByDnsLookup()) {
            getBrokerController().getBrokerOuterAPI().updateNameServerAddressListByDnsLookup(this.brokerConfig.getNamesrvAddr());
        } else {
            getBrokerController().getBrokerOuterAPI().updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
        }
    }

    private void printMasterAndSlaveDiff() {
        MessageStore messageStore = getBrokerController().getMessageStore();
        if (messageStore.getHaService() != null && messageStore.getHaService().getConnectionCount().get() > 0) {
            long diff = messageStore.slaveFallBehindMuch();
            LOG.info("CommitLog: slave fall behind master {}bytes", diff);
        }
    }

    private void initPoolExecutors() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerScheduledThread", true, getBrokerController().getBrokerIdentity()));

        this.syncBrokerMemberGroupExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", getBrokerController().getBrokerIdentity()));
        this.brokerHeartbeatExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerHeartbeatScheduledThread", getBrokerController().getBrokerIdentity()));
    }


    public ScheduledExecutorService getSyncBrokerMemberGroupExecutorService() {
        return syncBrokerMemberGroupExecutorService;
    }

    public ScheduledExecutorService getBrokerHeartbeatExecutorService() {
        return brokerHeartbeatExecutorService;
    }

    public List<ScheduledFuture<?>> getScheduledFutures() {
        return scheduledFutures;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

}
