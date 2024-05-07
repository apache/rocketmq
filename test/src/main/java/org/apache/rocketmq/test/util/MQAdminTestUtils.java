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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingOne;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpc.ClientMetadata;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.topic.RemappingStaticTopicSubCommand;
import org.apache.rocketmq.tools.command.topic.UpdateStaticTopicSubCommand;

import static org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils.getMappingDetailFromConfig;
import static org.awaitility.Awaitility.await;

public class MQAdminTestUtils {
    private static Logger log = LoggerFactory.getLogger(MQAdminTestUtils.class);

    private static DefaultMQAdminExt mqAdminExt;

    public static void startAdmin(String nameSrvAddr) throws MQClientException {
        mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        mqAdminExt.start();
    }

    public static void shutdownAdmin() {
        mqAdminExt.shutdown();
    }

    public static boolean createTopic(String nameSrvAddr, String clusterName, String topic,
                                      int queueNum, Map<String, String> attributes) {
        int defaultWaitTime = 30;
        return createTopic(nameSrvAddr, clusterName, topic, queueNum, attributes, defaultWaitTime);
    }

    public static boolean createTopic(String nameSrvAddr, String clusterName, String topic,
                                      int queueNum, Map<String, String> attributes, int waitTimeSec) {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        try {
            mqAdminExt.start();
            mqAdminExt.createTopic(clusterName, topic, queueNum, attributes);
        } catch (Exception e) {
        }

        await().atMost(waitTimeSec, TimeUnit.SECONDS).until(() -> checkTopicExist(mqAdminExt, topic));
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
        return true;
    }

    public static boolean checkTopicExist(DefaultMQAdminExt mqAdminExt, String topic) {
        boolean createResult = false;
        try {
            TopicStatsTable topicInfo = mqAdminExt.examineTopicStats(topic);
            createResult = !topicInfo.getOffsetTable().isEmpty();
        } catch (Exception e) {
        }

        return createResult;
    }

    public static boolean createSub(String nameSrvAddr, String clusterName, String consumerId) {
        boolean createResult = true;
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(consumerId);
        try {
            mqAdminExt.start();
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt,
                    clusterName);
            for (String addr : masterSet) {
                try {
                    mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, config);
                    log.info("create subscription group {} to {} success.", consumerId, addr);
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000 * 1);
                }
            }
        } catch (Exception e) {
            createResult = false;
            e.printStackTrace();
        }
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
        return createResult;
    }

    public static ClusterInfo getCluster(String nameSrvAddr) {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        ClusterInfo clusterInfo = null;
        try {
            mqAdminExt.start();
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
        return clusterInfo;
    }

    public static boolean isBrokerExist(String ns, String ip) {
        ClusterInfo clusterInfo = getCluster(ns);
        if (clusterInfo == null) {
            return false;
        } else {
            Map<String, BrokerData> brokers = clusterInfo.getBrokerAddrTable();
            for (String brokerName : brokers.keySet()) {
                HashMap<Long, String> brokerIps = brokers.get(brokerName).getBrokerAddrs();
                for (long brokerId : brokerIps.keySet()) {
                    if (brokerIps.get(brokerId).contains(ip))
                        return true;
                }
            }
        }

        return false;
    }


    public static boolean awaitStaticTopicMs(long timeMs, String topic, DefaultMQAdminExt defaultMQAdminExt, MQClientInstance clientInstance) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= timeMs) {
            if (checkStaticTopic(topic, defaultMQAdminExt, clientInstance)) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    //Check if the client metadata is consistent with server metadata
    public static boolean checkStaticTopic(String topic, DefaultMQAdminExt defaultMQAdminExt, MQClientInstance clientInstance) throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
        assert !brokerConfigMap.isEmpty();
        TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
        TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
        for (int i = 0; i < globalIdMap.size(); i++) {
            TopicQueueMappingOne mappingOne = globalIdMap.get(i);
            String mockBrokerName = TopicQueueMappingUtils.getMockBrokerName(mappingOne.getMappingDetail().getScope());
            String bnameFromRoute = clientInstance.getBrokerNameFromMessageQueue(new MessageQueue(topic, mockBrokerName, mappingOne.getGlobalId()));
            if (!mappingOne.getBname().equals(bnameFromRoute)) {
                return false;
            }
        }
        return  true;
    }

    //should only be test, if some middle operation failed, it dose not backup the brokerConfigMap
    public static Map<String, TopicConfigAndQueueMapping> createStaticTopic(String topic, int queueNum, Set<String> targetBrokers, DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
        assert brokerConfigMap.isEmpty();
        TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
        MQAdminUtils.completeNoTargetBrokers(brokerConfigMap, defaultMQAdminExt);
        MQAdminUtils.updateTopicConfigMappingAll(brokerConfigMap, defaultMQAdminExt, false);
        return brokerConfigMap;
    }

    //should only be test, if some middle operation failed, it dose not backup the brokerConfigMap
    public static void remappingStaticTopic(String topic, Set<String> targetBrokers, DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
        assert !brokerConfigMap.isEmpty();
        TopicRemappingDetailWrapper wrapper = TopicQueueMappingUtils.remappingStaticTopic(topic, brokerConfigMap, targetBrokers);
        MQAdminUtils.completeNoTargetBrokers(brokerConfigMap, defaultMQAdminExt);
        MQAdminUtils.remappingStaticTopic(topic, wrapper.getBrokerToMapIn(), wrapper.getBrokerToMapOut(), brokerConfigMap, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, false, defaultMQAdminExt);
    }


    //for test only
    public static void remappingStaticTopicWithNegativeLogicOffset(String topic, Set<String> targetBrokers, DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
        assert !brokerConfigMap.isEmpty();
        TopicRemappingDetailWrapper wrapper = TopicQueueMappingUtils.remappingStaticTopic(topic, brokerConfigMap, targetBrokers);
        MQAdminUtils.completeNoTargetBrokers(brokerConfigMap, defaultMQAdminExt);
        remappingStaticTopicWithNegativeLogicOffset(topic, wrapper.getBrokerToMapIn(), wrapper.getBrokerToMapOut(), brokerConfigMap, TopicQueueMappingUtils.DEFAULT_BLOCK_SEQ_SIZE, false, defaultMQAdminExt);
    }

    //for test only
    public static void remappingStaticTopicWithNegativeLogicOffset(String topic, Set<String> brokersToMapIn, Set<String> brokersToMapOut, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, int blockSeqSize, boolean force, DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        ClientMetadata clientMetadata = MQAdminUtils.getBrokerMetadata(defaultMQAdminExt);
        MQAdminUtils.checkIfMasterAlive(brokerConfigMap.keySet(), defaultMQAdminExt, clientMetadata);
        // now do the remapping
        //Step1: let the new leader can be write without the logicOffset
        for (String broker : brokersToMapIn) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
        //Step2: forbid the write of old leader
        for (String broker : brokersToMapOut) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }

        //Step5: write the non-target brokers
        for (String broker : brokerConfigMap.keySet()) {
            if (brokersToMapIn.contains(broker) || brokersToMapOut.contains(broker)) {
                continue;
            }
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
    }

    public static void createStaticTopicWithCommand(String topic, int queueNum, Set<String> brokers, String cluster, String nameservers) throws Exception {
        UpdateStaticTopicSubCommand cmd = new UpdateStaticTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] args;
        if (cluster != null) {
            args = new String[]{
                "-c", cluster,
                "-t", topic,
                "-qn", String.valueOf(queueNum),
                "-n", nameservers
            };
        } else {
            String brokerStr = String.join(",", brokers);
            args = new String[]{
                "-b", brokerStr,
                "-t", topic,
                "-qn", String.valueOf(queueNum),
                "-n", nameservers
            };
        }
        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), args, cmd.buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            return;
        }
        if (commandLine.hasOption('n')) {
            String namesrvAddr = commandLine.getOptionValue('n');
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
        }
        cmd.execute(commandLine, options, null);
    }

    public static void remappingStaticTopicWithCommand(String topic, Set<String> brokers, String cluster, String nameservers) throws Exception {
        RemappingStaticTopicSubCommand cmd = new RemappingStaticTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] args;
        if (cluster != null) {
            args = new String[]{
                "-c", cluster,
                "-t", topic,
                "-n", nameservers
            };
        } else {
            String brokerStr = String.join(",", brokers);
            args = new String[]{
                "-b", brokerStr,
                "-t", topic,
                "-n", nameservers
            };
        }
        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), args, cmd.buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            return;
        }
        if (commandLine.hasOption('n')) {
            String namesrvAddr = commandLine.getOptionValue('n');
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
        }
        cmd.execute(commandLine, options, null);
    }

    public static ConsumeStats examineConsumeStats(String brokerAddr, String topic, String group) {
        ConsumeStats consumeStats = null;
        try {
            consumeStats = mqAdminExt.examineConsumeStats(brokerAddr, group, topic, 3000);
        } catch (Exception ignored) {
        }
        return consumeStats;
    }

    /**
     * Delete topic from broker only without cleaning route info from name server forwardly
     *
     * @param nameSrvAddr the namesrv addr to connect
     * @param brokerName the specific broker
     * @param topic the specific topic to delete
     */
    public static void deleteTopicFromBrokerOnly(String nameSrvAddr, String brokerName, String topic) {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);

        try {
            mqAdminExt.start();
            String brokerAddr = CommandUtil.fetchMasterAddrByBrokerName(mqAdminExt, brokerName);
            mqAdminExt.deleteTopicInBroker(Collections.singleton(brokerAddr), topic);
        } catch (Exception ignored) {
        } finally {
            mqAdminExt.shutdown();
        }
    }

    public static TopicRouteData examineTopicRouteInfo(String nameSrvAddr, String topicName) {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        TopicRouteData route = null;
        try {
            mqAdminExt.start();
            route = mqAdminExt.examineTopicRouteInfo(topicName);
        } catch (Exception ignored) {
        } finally {
            mqAdminExt.shutdown();
        }
        return route;
    }
}
