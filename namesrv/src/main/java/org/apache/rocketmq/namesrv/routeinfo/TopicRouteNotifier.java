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

import com.alibaba.fastjson.JSON;
import com.sun.management.OperatingSystemMXBean;
import io.netty.channel.Channel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.namesrv.UpdateTopicRouteRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * if topic route info changed, then notify client scheduled
 */
public class TopicRouteNotifier {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final RouteInfoManager routeInfoManager;

    private final RemotingServer remotingServer;

    public TopicRouteNotifier(RemotingServer remotingServer, RouteInfoManager routeInfoManager) {
        this.routeInfoManager = routeInfoManager;
        this.remotingServer = remotingServer;
    }

    /**
     * if topic route info has changed in the period, then notify client
     */
    public void notifyClients() {
        if (isSystemBusy()) {
            return;
        }
        Map<String, Set<Channel>> topicAndChannelMap = routeInfoManager.getAndResetChangedTopicMap();
        if (MapUtils.isEmpty(topicAndChannelMap)) {
            return;
        }

        // convert map structure
        Map<Channel, Set<String>> channelAndTopicMap = transToChannelMap(topicAndChannelMap);

        for (Map.Entry<Channel, Set<String>> entry : channelAndTopicMap.entrySet()) {
            Channel channel = entry.getKey();
            Set<String> topicSet = entry.getValue();
            notifyClientMultiTopics(channel, topicSet);
        }
    }

    /**
     * convert channel and topic,  'one channel'  VS  'multi topic'
     */
    private Map<Channel, Set<String>> transToChannelMap(Map<String, Set<Channel>> topicAndChannelMap) {
        Map<Channel, Set<String>> channelAndTopicMap = new HashMap<>();
        for (Map.Entry<String, Set<Channel>> entry : topicAndChannelMap.entrySet()) {
            String topic = entry.getKey();
            Set<Channel> channelSet = entry.getValue();
            for (Channel channel : channelSet) {
                Set<String> topicSet = channelAndTopicMap.computeIfAbsent(channel, (k) -> new HashSet<>());
                topicSet.add(topic);
            }
        }
        return channelAndTopicMap;
    }

    private void notifyClientMultiTopics(Channel clientChannel, Set<String> topicSet) {
        if (clientChannel == null || CollectionUtils.isEmpty(topicSet)) {
            return;
        }
        RemotingCommand remotingCommand = transToCommand(topicSet);
        try {
            remotingServer.invokeOneway(clientChannel, remotingCommand, 50);
        } catch (Exception e) {
            log.error("invoke client exception. topicSet={}, channel={}, error={}",
                    topicSet, clientChannel, e.toString());
        }
    }

    /**
     * append command which notify client the topic content changed
     * command only contains header, just notify client to Actively pull data
     *
     * @param topicSet multi topic
     * @return  command obj
     */
    private RemotingCommand transToCommand(Set<String> topicSet) {
        UpdateTopicRouteRequestHeader header = new UpdateTopicRouteRequestHeader();
        header.setTopics(StringUtils.join(topicSet, CommonConstants.COMMA));
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.NOTIFY_TOPIC_ROUTE_CHANGED, header);
        request.setRemark(null);
        return request;
    }

    /**
     * judge whether the current system is busy
     *
     * @return  true: busy    false: not busy
     */
    private boolean isSystemBusy() {
        OperatingSystemMXBean systemMXBean;
        java.lang.management.OperatingSystemMXBean system = ManagementFactory.getOperatingSystemMXBean();
        if (system instanceof OperatingSystemMXBean) {
            systemMXBean = (OperatingSystemMXBean) system;
        } else {
            return false;
        }
        return isMemoryBusy(systemMXBean) || isLoadBusy(systemMXBean);
    }

    private boolean isLoadBusy(OperatingSystemMXBean systemMXBean) {
        int processors = systemMXBean.getAvailableProcessors();
        double loadAverage = systemMXBean.getSystemLoadAverage();
        boolean isLoadBusy = loadAverage / processors > 0.9D;
        if (isLoadBusy) {
            log.warn("load busy, processors {}, loadAverage {}", processors, loadAverage);
        }
        return isLoadBusy;
    }

    private boolean isMemoryBusy(OperatingSystemMXBean systemMXBean) {
        double totalMemorySize = ((Long) (systemMXBean.getTotalPhysicalMemorySize())).doubleValue();
        double freeMemorySize = ((Long) (systemMXBean.getFreePhysicalMemorySize())).doubleValue();
        double freeSwapSize = ((Long) (systemMXBean.getFreeSwapSpaceSize())).doubleValue();

        boolean isMemoryBusy = (freeMemorySize + freeSwapSize) / totalMemorySize < 0.05D;
        if (isMemoryBusy) {
            log.warn("memory busy, totalMemorySize {}, freeMemorySize {}, freeSwapSize {}",
                    formatMemory(totalMemorySize), formatMemory(freeMemorySize), formatMemory(freeSwapSize));
        }
        return isMemoryBusy;
    }

    private String formatMemory(double memory) {
        memory = memory / 1024 / 1024 / 1024;
        return String.format("%.2f", memory);
    }

}
