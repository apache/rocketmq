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
package org.apache.rocketmq.broker.tiered;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.index.DispatchRequestExt;
import org.apache.rocketmq.tieredstore.index.IndexStoreService;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncTieredIndexService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger TIERED_LOG = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final BrokerOuterAPI brokerOuterAPI;
    private final TieredMessageStore messageStore;
    private final MessageStoreConfig messageStoreConfig;
    private final org.apache.rocketmq.tieredstore.MessageStoreConfig tieredConfig;
    private final String brokerAddr;
    private final IndexStoreService indexStoreService;
    private final List<String> peerAddrs;
    private final ExecutorService syncDispatchRequestExecutor;
    public SyncTieredIndexService(BrokerController brokerController) {
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
        this.messageStore = (TieredMessageStore) brokerController.getMessageStore();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.tieredConfig = messageStore.getStoreConfig();
        this.brokerAddr = brokerController.getBrokerAddr();
        this.peerAddrs = new ArrayList<>();
        this.indexStoreService = (IndexStoreService) messageStore.getIndexService();
        this.syncDispatchRequestExecutor = new ThreadPoolExecutor(
            tieredConfig.getSyncDispatchRequestThreadPoolSize(),
            tieredConfig.getSyncDispatchRequestThreadPoolSize(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(tieredConfig.getSyncDispatchRequestQueueCapacity()),
            new ThreadFactoryImpl("SyncDispatchRequestExecutor_")
        );

        String dLedgerPeers = messageStoreConfig.getdLegerPeers();
        String[] dLedgerPeersArray = dLedgerPeers.split(";");
        Pattern pattern = Pattern.compile("-([^:]+):");
        for (String dLedgerPeer : dLedgerPeersArray) {
            Matcher matcher = pattern.matcher(dLedgerPeer);
            if (matcher.find()) {
                String peerIp = matcher.group(1);
                int listenPort = brokerController.getNettyServerConfig().getListenPort();
                this.peerAddrs.add(String.format("%s:%s", peerIp, listenPort));
            }
        }
    }

    public void syncTieredIndex() {
        if (MessageStoreUtil.isMaster(messageStoreConfig)) {
            while (indexStoreService.needToSync()) {
                if (MessageStoreUtil.isMaster(messageStoreConfig)) {
                    List<DispatchRequestExt> requestExtList = indexStoreService.getDispatchRequestToSync();
                    if (!requestExtList.isEmpty()) {
                        CompletableFuture.runAsync(() -> peerAddrs.forEach(peerAddr -> {
                            if (!peerAddr.equals(brokerAddr)) {
                                try {
                                    RemotingCommand response = brokerOuterAPI.syncTieredIndex(requestExtList, peerAddr);
                                    if (response == null) {
                                        TIERED_LOG.error("syncTieredIndex failed, no response, peer = {}", peerAddr);
                                    } else if (response.getCode() != ResponseCode.SUCCESS) {
                                        TIERED_LOG.error("syncTieredIndex failed, peer = {}, code = {}, remark = {}", peerAddr, response.getCode(), response.getRemark());
                                    }
                                } catch (Exception e) {
                                    TIERED_LOG.error("syncTieredIndex failed, peer = {}", peerAddr, e);
                                }
                            }
                        }), syncDispatchRequestExecutor);
                    }
                }
            }
        }
    }

    @Override
    public String getServiceName() {
        return SyncTieredIndexService.class.getSimpleName();
    }

    @Override
    public void run() {
        TIERED_LOG.info("SyncTieredIndexService start, peerAddrs = {}", peerAddrs);
        while (!this.isStopped()) {
            try {
                this.waitForRunning(50);
                syncTieredIndex();
            } catch (Throwable t) {
                TIERED_LOG.error("Synchronize tiered IndexFile service failed", t);
            }
        }
        TIERED_LOG.info("End synchronize tiered IndexFile service thread!");
    }
}
