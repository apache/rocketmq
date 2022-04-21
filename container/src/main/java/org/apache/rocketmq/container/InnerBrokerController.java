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
package org.apache.rocketmq.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class InnerBrokerController extends BrokerController {
    private ScheduledExecutorService syncBrokerMemberGroupExecutorService;
    private ScheduledExecutorService brokerHeartbeatExecutorService;
    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;
    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    private BrokerPreOnlineService brokerPreOnlineService;
    protected BrokerContainer brokerContainer;
    protected volatile boolean isIsolated = false;

    public InnerBrokerController(
        final BrokerContainer brokerContainer,
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        super(brokerConfig, messageStoreConfig);
        this.brokerContainer = brokerContainer;
        this.brokerOuterAPI = this.brokerContainer.getBrokerOuterAPI();

        if (!this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }
    }

    @Override
    protected void initializeRemotingServer() {
        this.remotingServer = this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort());
        this.fastRemotingServer = this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort() - 2);
    }

    /**
     * Initialize resources for master which will be re-used by slave.
     */
    @Override
    protected void initializeResources() {
        super.initializeResources();
        this.syncBrokerMemberGroupExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", super.getBrokerIdentity()));
        this.brokerHeartbeatExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("rokerControllerHeartbeatScheduledThread", super.getBrokerIdentity()));
    }

    @Override
    protected void initializeScheduledTasks() {
        initializeBrokerScheduledTasks();
    }

    @Override
    public void shutdown() {

        shutdownBasicService();

        if (this.remotingServer != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort());
        }

        if (this.fastRemotingServer != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort() - 2);
        }

        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        if (this.brokerPreOnlineService != null && !this.brokerPreOnlineService.isStopped()) {
            this.brokerPreOnlineService.shutdown();
        }

        shutdownScheduledExecutorService(this.syncBrokerMemberGroupExecutorService);
        shutdownScheduledExecutorService(this.brokerHeartbeatExecutorService);

    }

    @Override
    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.brokerConfig.getListenPort();
    }

    @Override
    public void start() throws Exception {

        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1) {
            isIsolated = true;
        }

        startBasicService();

        if (this.brokerPreOnlineService != null) {
            this.brokerPreOnlineService.start();
        }

        if (!isIsolated) {
            this.registerBrokerAll(true, false, true);
        }

        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        BrokerController.LOG.info("Register to namesrv after {}", shouldStartTime);
                        return;
                    }
                    if (isIsolated) {
                        BrokerController.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    InnerBrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    BrokerController.LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            scheduledFutures.add(this.brokerHeartbeatExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run2() {
                    if (isIsolated) {
                        return;
                    }
                    try {
                        InnerBrokerController.this.sendHeartbeat();
                    } catch (Exception e) {
                        BrokerController.LOG.error("sendHeartbeat Exception", e);
                    }

                }
            }, 1000, brokerConfig.getBrokerHeartbeatInterval(), TimeUnit.MILLISECONDS));

            scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override public void run2() {
                    try {
                        InnerBrokerController.this.syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        BrokerController.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));

        }

        if (!isIsolated && !messageStoreConfig.isEnableDLegerCommitLog()
            && !messageStoreConfig.isDuplicationEnable() && !this.brokerConfig.isEnableSlaveActingMaster()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        }

        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    private void sendHeartbeat() {
        if (this.brokerContainer.getBrokerContainerConfig().isCompatibleWithOldNameSrv()) {
            this.brokerOuterAPI.sendHeartbeatViaDataVersion(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                this.getTopicConfigManager().getDataVersion(),
                this.brokerConfig.isInBrokerContainer());
        } else {
            this.brokerOuterAPI.sendHeartbeat(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                this.brokerConfig.isInBrokerContainer());
        }
    }

    public void syncBrokerMemberGroup() {
        try {
            brokerMemberGroup = this.brokerContainer.getBrokerOuterAPI()
                .syncBrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerContainer.getBrokerContainerConfig().isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            BrokerController.LOG.error("syncBrokerMemberGroup from namesrv failed, ", e);
            return;
        }
        if (brokerMemberGroup == null || brokerMemberGroup.getBrokerAddrs().size() == 0) {
            BrokerController.LOG.warn("Couldn't find any broker member from namesrv in {}/{}", this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
            return;
        }
        this.messageStore.setAliveReplicaNumInGroup(calcAliveBrokerNumInGroup(brokerMemberGroup.getBrokerAddrs()));

        if (!this.isIsolated) {
            long minBrokerId = brokerMemberGroup.minimumBrokerId();
            this.updateMinBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
    }

    private int calcAliveBrokerNumInGroup(Map<Long, String> brokerAddrTable) {
        if (brokerAddrTable.containsKey(this.brokerConfig.getBrokerId())) {
            return brokerAddrTable.size();
        } else {
            return brokerAddrTable.size() + 1;
        }
    }

    @Override
    protected void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
        TopicConfigSerializeWrapper topicConfigWrapper) {

        if (shutdown) {
            BrokerController.LOG.info("BrokerController#doResterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.getHAServerAddr(),
            topicConfigWrapper,
            this.filterServerManager.buildNewFilterServerList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            this.getBrokerIdentity());

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    @Override
    public String getNameServerList() {
        if (this.brokerContainer.getBrokerContainerConfig().getNamesrvAddr() != null) {
            this.brokerContainer.getBrokerOuterAPI().updateNameServerAddressList(brokerContainer.getBrokerContainerConfig().getNamesrvAddr());
            return this.brokerContainer.getBrokerContainerConfig().getNamesrvAddr();
        } else if (this.brokerContainer.getBrokerContainerConfig().isFetchNamesrvAddrByAddressServer()) {
            return this.brokerContainer.getBrokerOuterAPI().fetchNameServerAddr();
        }
        return null;
    }

    @Override
    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    @Override
    public long getMinBrokerIdInGroup() {
        return this.minBrokerIdInGroup;
    }

    @Override
    public boolean isSpecialServiceRunning() {
        if (isScheduleServiceStart() && isTransactionCheckServiceStart()) {
            return true;
        }

        return this.ackMessageProcessor != null && this.ackMessageProcessor.isPopReviveServiceRunning();
    }

    @Override
    public int getListenPort() {
        return this.brokerConfig.getListenPort();
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerContainer.getBrokerOuterAPI();
    }

    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.changeSpecialServiceStatus(false);
        this.closeChannels();
    }

    public synchronized void closeChannels() {
        this.brokerContainer.getBrokerOuterAPI().getRemotingClient().closeChannels();
    }

    public BrokerContainer getBrokerContainer() {
        return this.brokerContainer;
    }

    public boolean isIsolated() {
        return this.isIsolated;
    }

    public NettyServerConfig getNettyServerConfig() {
        return brokerContainer.getNettyServerConfig();
    }

    public NettyClientConfig getNettyClientConfig() {
        return brokerContainer.getNettyClientConfig();
    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        BrokerController brokerController = this.brokerContainer.findBrokerControllerByBrokerName(brokerName);
        if (brokerController != null) {
            return brokerController.getMessageStore();
        }
        return null;
    }

    @Override
    public BrokerController peekMasterBroker() {
        if (brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
            return this;
        }
        return this.brokerContainer.peekMasterBroker();
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }
}
