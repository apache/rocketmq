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
package org.apache.rocketmq.namesrv.routeinfo;

import com.sun.management.OperatingSystemMXBean;
import io.netty.channel.Channel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.namesrv.UpdateTopicRouteRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;

/**
 * if topic route info changed, then notify client scheduled
 */
public class TopicRouteNotifier {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final RouteInfoManager routeInfoManager;

    private final RemotingServer remotingServer;

    private static volatile boolean SYSTEM_BUSY_FLAG = false;

    private static volatile Long LAST_CACHE_TIME = null;

    public TopicRouteNotifier(RemotingServer remotingServer, RouteInfoManager routeInfoManager) {
        this.routeInfoManager = routeInfoManager;
        this.remotingServer = remotingServer;
    }

    /**
     * if topic route info has changed in the period, then notify client
     */
    public void notifyClients() {
        System.out.println("isSystemBusy() is : " + isSystemBusy());
        if (isSystemBusy()) {
            return;
        }
        Map<String, Set<Channel>> topicAndChannelMap = routeInfoManager.getAndResetChangedTopicMap();
        if (MapUtils.isEmpty(topicAndChannelMap)) {
            return;
        }

        for (Map.Entry<String, Set<Channel>> entry : topicAndChannelMap.entrySet()) {
            notifyClientsByTopic(entry.getKey(), entry.getValue());
        }
    }

    private void notifyClientsByTopic(String topic, Set<Channel> channelSet) {
        if (topic == null || CollectionUtils.isEmpty(channelSet)) {
            return;
        }
        for (Channel channel : channelSet) {
            RemotingCommand remotingCommand = transToCommand(topic);
            try {
                remotingServer.invokeOneway(channel, remotingCommand, 50);
            } catch (Exception e) {
                log.error("invoke client exception. topic={}, channel={}, error={}", topic, channel, e.toString());
            }
        }
    }

    private RemotingCommand transToCommand(String topic) {
        UpdateTopicRouteRequestHeader header = new UpdateTopicRouteRequestHeader();
        header.setTopic(topic);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CLIENT_TOPIC_ROUTE_CHANGED, header);
        request.setRemark(null);
        return request;
    }

    private boolean isSystemBusy() {
        if (LAST_CACHE_TIME != null && System.currentTimeMillis() - LAST_CACHE_TIME < 5 * 60 * 1000) {
            return SYSTEM_BUSY_FLAG;
        }
        OperatingSystemMXBean systemMXBean;
        java.lang.management.OperatingSystemMXBean system = ManagementFactory.getOperatingSystemMXBean();
        if (system instanceof OperatingSystemMXBean) {
            systemMXBean = (OperatingSystemMXBean) system;
        } else {
            LAST_CACHE_TIME = System.currentTimeMillis();
            SYSTEM_BUSY_FLAG = false;
            return SYSTEM_BUSY_FLAG;
        }

        double totalMemorySize = ((Long) (systemMXBean.getTotalPhysicalMemorySize())).doubleValue();
        double freeMemorySize = ((Long) (systemMXBean.getFreePhysicalMemorySize())).doubleValue();
        double freeSwapSize = ((Long) (systemMXBean.getFreeSwapSpaceSize())).doubleValue();
        boolean isMemoryBusy = (freeMemorySize + freeSwapSize) / totalMemorySize < 0.05D;
        int processors = systemMXBean.getAvailableProcessors();
        double loadAverage = systemMXBean.getSystemLoadAverage();
        boolean isLoadBusy = loadAverage / processors < 0.05D;
        SYSTEM_BUSY_FLAG = isMemoryBusy || isLoadBusy;
        LAST_CACHE_TIME = System.currentTimeMillis();
        return SYSTEM_BUSY_FLAG;
    }

}
