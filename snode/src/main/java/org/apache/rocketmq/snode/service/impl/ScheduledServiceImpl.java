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
package org.apache.rocketmq.snode.service.impl;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SnodeData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.service.ScheduledService;

public class ScheduledServiceImpl implements ScheduledService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private SnodeController snodeController;

    private SnodeConfig snodeConfig;

    private final RemotingCommand enodeHeartbeat;

    public ScheduledServiceImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
        this.snodeConfig = snodeController.getSnodeConfig();
        enodeHeartbeat = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(snodeConfig.getSnodeName());
        SnodeData snodeData = new SnodeData();
        snodeData.setSnodeName(snodeConfig.getSnodeName());
        heartbeatData.setSnodeData(snodeData);
        enodeHeartbeat.setBody(heartbeatData.encode());
    }

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "SNodeScheduledThread");
        }
    });

    @Override
    public void startScheduleTask() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    snodeController.getEnodeService().sendHearbeat(enodeHeartbeat);
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 0, this.snodeConfig.getSnodeHeartBeatInterval(), TimeUnit.MILLISECONDS);

        if (snodeConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        snodeController.getNnodeService().fetchNnodeAdress();
                    } catch (Throwable e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                snodeController.getNnodeService().registerSnode(snodeConfig);
            }
        }, 0, Math.max(10000, Math.min(snodeConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    snodeController.getEnodeService().updateEnodeAddress(snodeConfig.getClusterName());
                } catch (Exception ex) {
                    log.warn("Update broker addr error:{}", ex);
                }
            }
        }, 0, Math.max(10000, Math.min(snodeConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    snodeController.getNnodeService().updateTopicRouteDataByTopic();
                } catch (Exception ex) {
                    log.warn("Update broker addr error:{}", ex);
                }
            }
        }, 0, Math.max(10000, Math.min(snodeConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    snodeController.getNnodeService().updateEnodeClusterInfo();
                } catch (Exception ex) {
                    log.warn("Update broker addr error:{}", ex);
                }
            }
        }, 0, Math.max(10000, Math.min(snodeConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    snodeController.getConsumerOffsetManager().persist();
                } catch (Throwable e) {
                    log.error("ScheduledTask fetchNameServerAddr exception", e);
                }
            }
        }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdown();
        }
    }
}
