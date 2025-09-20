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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class InnerBrokerController extends BrokerController {
    protected BrokerContainer brokerContainer;

    public InnerBrokerController(
        final BrokerContainer brokerContainer,
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig,
        final AuthConfig authConfig
    ) {
        super(brokerConfig, messageStoreConfig, authConfig);
        this.brokerContainer = brokerContainer;
        this.brokerOuterAPI = this.brokerContainer.getBrokerOuterAPI();
    }

    @Override
    protected void initializeRemotingServer() {
        RemotingServer remotingServer = this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort());
        RemotingServer fastRemotingServer = this.brokerContainer.getRemotingServer().newRemotingServer(brokerConfig.getListenPort() - 2);

        setRemotingServer(remotingServer);
        setFastRemotingServer(fastRemotingServer);
    }

    @Override
    protected void initializeScheduledTasks() {
        initializeBrokerScheduledTasks();
    }

    @Override
    public void start() throws Exception {
        this.startupTime = System.currentTimeMillis();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.registerBrokerAll(true, false, true);
        }

        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    long diff = System.currentTimeMillis() - startupTime;
                    if (diff >= 0 && diff < messageStoreConfig.getDisappearTimeAfterStart()) {
                        BrokerController.LOG.info("Register to namesrv after {}", startupTime + messageStoreConfig.getDisappearTimeAfterStart());
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
            scheduleSendHeartbeat();

            scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run0() {
                    try {
                        InnerBrokerController.this.syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        BrokerController.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }

        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    @Override
    public void shutdown() {

        shutdownBasicService();

        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        if (getRemotingServer() != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort());
        }

        if (getFastRemotingServer() != null) {
            this.brokerContainer.getRemotingServer().removeRemotingServer(brokerConfig.getListenPort() - 2);
        }
    }

    @Override
    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.brokerConfig.getListenPort();
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
    public int getListenPort() {
        return this.brokerConfig.getListenPort();
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerContainer.getBrokerOuterAPI();
    }

    public BrokerContainer getBrokerContainer() {
        return this.brokerContainer;
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
}
