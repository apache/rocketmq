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
package org.apache.rocketmq.remoting.protocol.statictopic;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;

public class TopicQueueMappingUtils {

    public static final int DEFAULT_BLOCK_SEQ_SIZE = 10000;

    public static class MappingAllocator {
        Map<String, Integer> brokerNumMap = new HashMap<>();
        Map<Integer, String> idToBroker = new HashMap<>();
        //used for remapping
        Map<String, Integer> brokerNumMapBeforeRemapping;
        int currentIndex = 0;
        List<String> leastBrokers = new ArrayList<>();
        private MappingAllocator(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap, Map<String, Integer> brokerNumMapBeforeRemapping) {
            this.idToBroker.putAll(idToBroker);
            this.brokerNumMap.putAll(brokerNumMap);
            this.brokerNumMapBeforeRemapping = brokerNumMapBeforeRemapping;
        }

        private void freshState() {
            int minNum = Integer.MAX_VALUE;
            for (Map.Entry<String, Integer> entry : brokerNumMap.entrySet()) {
                if (entry.getValue() < minNum) {
                    leastBrokers.clear();
                    leastBrokers.add(entry.getKey());
                    minNum = entry.getValue();
                } else if (entry.getValue() == minNum) {
                    leastBrokers.add(entry.getKey());
                }
            }
            //reduce the remapping
            if (brokerNumMapBeforeRemapping != null
                    && !brokerNumMapBeforeRemapping.isEmpty()) {
                leastBrokers.sort((o1, o2) -> {
                    int i1 = 0, i2 = 0;
                    if (brokerNumMapBeforeRemapping.containsKey(o1)) {
                        i1 = brokerNumMapBeforeRemapping.get(o1);
                    }
                    if (brokerNumMapBeforeRemapping.containsKey(o2)) {
                        i2 = brokerNumMapBeforeRemapping.get(o2);
                    }
                    return i1 - i2;
                });
            } else {
                //reduce the imbalance
                Collections.shuffle(leastBrokers);
            }
            currentIndex = leastBrokers.size() - 1;
        }
        private String nextBroker() {
            if (leastBrokers.isEmpty()) {
                freshState();
            }
            int tmpIndex = currentIndex % leastBrokers.size();
            return leastBrokers.remove(tmpIndex);
        }

        public Map<String, Integer> getBrokerNumMap() {
            return brokerNumMap;
        }

        public void upToNum(int maxQueueNum) {
            int currSize = idToBroker.size();
            if (maxQueueNum <= currSize) {
                return;
            }
            for (int i = currSize; i < maxQueueNum; i++) {
                String nextBroker = nextBroker();
                if (brokerNumMap.containsKey(nextBroker)) {
                    brokerNumMap.put(nextBroker, brokerNumMap.get(nextBroker) + 1);
                } else {
                    brokerNumMap.put(nextBroker, 1);
                }
                idToBroker.put(i, nextBroker);
            }
        }

        public Map<Integer, String> getIdToBroker() {
            return idToBroker;
        }
    }


    public static MappingAllocator buildMappingAllocator(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap, Map<String, Integer> brokerNumMapBeforeRemapping) {
        return new MappingAllocator(idToBroker, brokerNumMap, brokerNumMapBeforeRemapping);
    }

    public static Map.Entry<Long, Integer> findMaxEpochAndQueueNum(List<TopicQueueMappingDetail> mappingDetailList) {
        long epoch = -1;
        int queueNum = 0;
        for (TopicQueueMappingDetail mappingDetail : mappingDetailList) {
            if (mappingDetail.getEpoch() > epoch) {
                epoch = mappingDetail.getEpoch();
            }
            if (mappingDetail.getTotalQueues() > queueNum) {
                queueNum = mappingDetail.getTotalQueues();
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(epoch, queueNum);
    }

    public static List<TopicQueueMappingDetail> getMappingDetailFromConfig(Collection<TopicConfigAndQueueMapping> configs) {
        List<TopicQueueMappingDetail> detailList = new ArrayList<>();
        for (TopicConfigAndQueueMapping configMapping : configs) {
            if (configMapping.getMappingDetail() != null) {
                detailList.add(configMapping.getMappingDetail());
            }
        }
        return detailList;
    }

    public static Map.Entry<Long, Integer> checkNameEpochNumConsistence(String topic, Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        if (brokerConfigMap == null
            || brokerConfigMap.isEmpty()) {
            return null;
        }
        //make sure it is not null
        long maxEpoch = -1;
        int maxNum = -1;
        String scope = null;
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            String broker = entry.getKey();
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            if (configMapping.getMappingDetail() == null) {
                throw new RuntimeException("Mapping info should not be null in broker " + broker);
            }
            TopicQueueMappingDetail mappingDetail = configMapping.getMappingDetail();
            if (!broker.equals(mappingDetail.getBname())) {
                throw new RuntimeException(String.format("The broker name is not equal %s != %s ", broker, mappingDetail.getBname()));
            }
            if (mappingDetail.isDirty()) {
                throw new RuntimeException("The mapping info is dirty in broker  " + broker);
            }
            if (!configMapping.getTopicName().equals(mappingDetail.getTopic())) {
                throw new RuntimeException("The topic name is inconsistent in broker  " + broker);
            }
            if (topic != null
                && !topic.equals(mappingDetail.getTopic())) {
                throw new RuntimeException("The topic name is not match for broker  " + broker);
            }

            if (scope != null
                && !scope.equals(mappingDetail.getScope())) {
                throw new RuntimeException(String.format("scope dose not match %s != %s in %s", mappingDetail.getScope(), scope, broker));
            } else {
                scope = mappingDetail.getScope();
            }

            if (maxEpoch != -1
                && maxEpoch != mappingDetail.getEpoch()) {
                throw new RuntimeException(String.format("epoch dose not match %d != %d in %s", maxEpoch, mappingDetail.getEpoch(), mappingDetail.getBname()));
            } else {
                maxEpoch = mappingDetail.getEpoch();
            }

            if (maxNum != -1
                && maxNum != mappingDetail.getTotalQueues()) {
                throw new RuntimeException(String.format("total queue number dose not match %d != %d in %s", maxNum, mappingDetail.getTotalQueues(), mappingDetail.getBname()));
            } else {
                maxNum = mappingDetail.getTotalQueues();
            }
        }
        return new AbstractMap.SimpleEntry<>(maxEpoch, maxNum);
    }

    public static String getMockBrokerName(String scope) {
        assert scope != null;
        if (scope.equals(MixAll.METADATA_SCOPE_GLOBAL)) {
            return MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope.substring(2);
        } else {
            return MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope;
        }
    }

    public static void makeSureLogicQueueMappingItemImmutable(List<LogicQueueMappingItem> oldItems, List<LogicQueueMappingItem> newItems, boolean epochEqual, boolean isCLean) {
        if (oldItems == null || oldItems.isEmpty()) {
            return;
        }
        if (newItems == null || newItems.isEmpty()) {
            throw new RuntimeException("The new item list is null or empty");
        }
        int iold = 0, inew = 0;
        while (iold < oldItems.size() && inew < newItems.size()) {
            LogicQueueMappingItem newItem = newItems.get(inew);
            LogicQueueMappingItem oldItem = oldItems.get(iold);
            if (newItem.getGen() < oldItem.getGen()) {
                //the earliest item may have been deleted concurrently
                inew++;
            } else if (oldItem.getGen() < newItem.getGen()) {
                //in the following cases, the new item-list has fewer items than old item-list
                //1. the queue is mapped back to a broker which hold the logic queue before
                //2. The earliest item is deleted by  TopicQueueMappingCleanService
                iold++;
            } else {
                assert oldItem.getBname().equals(newItem.getBname());
                assert oldItem.getQueueId() == newItem.getQueueId();
                assert oldItem.getStartOffset() == newItem.getStartOffset();
                if (oldItem.getLogicOffset() != -1) {
                    assert oldItem.getLogicOffset() == newItem.getLogicOffset();
                }
                iold++;
                inew++;
            }
        }
        if (epochEqual) {
            LogicQueueMappingItem oldLeader = oldItems.get(oldItems.size() - 1);
            LogicQueueMappingItem newLeader = newItems.get(newItems.size() - 1);
            if (newLeader.getGen() != oldLeader.getGen()
                || !newLeader.getBname().equals(oldLeader.getBname())
                || newLeader.getQueueId() != oldLeader.getQueueId()
                || newLeader.getStartOffset() != oldLeader.getStartOffset()) {
                throw new RuntimeException("The new leader is different but epoch equal");
            }
        }
    }


    public static void checkLogicQueueMappingItemOffset(List<LogicQueueMappingItem> items) {
        if (items == null
            || items.isEmpty()) {
            return;
        }
        int lastGen = -1;
        long lastOffset = -1;
        for (int i = items.size() - 1; i >= 0 ; i--) {
            LogicQueueMappingItem item = items.get(i);
            if (item.getStartOffset() < 0
                    || item.getGen() < 0
                    || item.getQueueId() < 0) {
                throw new RuntimeException("The field is illegal, should not be negative");
            }
            if (items.size() >= 2
                    && i <= items.size() - 2
                    && items.get(i).getLogicOffset() < 0) {
                throw new RuntimeException("The non-latest item has negative logic offset");
            }
            if (lastGen != -1 && item.getGen() >= lastGen) {
                throw new RuntimeException("The gen dose not increase monotonically");
            }

            if (item.getEndOffset() != -1
                && item.getEndOffset() < item.getStartOffset()) {
                throw new RuntimeException("The endOffset is smaller than the start offset");
            }

            if (lastOffset != -1 && item.getLogicOffset() != -1) {
                if (item.getLogicOffset() >= lastOffset) {
                    throw new RuntimeException("The base logic offset dose not increase monotonically");
                }
                if (item.computeMaxStaticQueueOffset() >= lastOffset) {
                    throw new RuntimeException("The max logic offset dose not increase monotonically");
                }
            }
            lastGen = item.getGen();
            lastOffset = item.getLogicOffset();
        }
    }

    public static void  checkIfReusePhysicalQueue(Collection<TopicQueueMappingOne> mappingOnes) {
        Map<String, TopicQueueMappingOne>  physicalQueueIdMap = new HashMap<>();
        for (TopicQueueMappingOne mappingOne : mappingOnes) {
            for (LogicQueueMappingItem item: mappingOne.items) {
                String physicalQueueId = item.getBname() + "-" + item.getQueueId();
                if (physicalQueueIdMap.containsKey(physicalQueueId)) {
                    throw new RuntimeException(String.format("Topic %s global queue id %d and %d shared the same physical queue %s",
                            mappingOne.topic, mappingOne.globalId, physicalQueueIdMap.get(physicalQueueId).globalId, physicalQueueId));
                } else {
                    physicalQueueIdMap.put(physicalQueueId, mappingOne);
                }
            }
        }
    }

    public static void  checkLeaderInTargetBrokers(Collection<TopicQueueMappingOne> mappingOnes, Set<String> targetBrokers) {
        for (TopicQueueMappingOne mappingOne : mappingOnes) {
            if (!targetBrokers.contains(mappingOne.bname)) {
                throw new RuntimeException("The leader broker does not in target broker");
            }
        }
    }

    public static void  checkPhysicalQueueConsistence(Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            assert configMapping != null;
            assert configMapping.getMappingDetail() != null;
            if (configMapping.getReadQueueNums() < configMapping.getWriteQueueNums()) {
                throw new RuntimeException("Read queues is smaller than write queues");
            }
            for (List<LogicQueueMappingItem> items: configMapping.getMappingDetail().getHostedQueues().values()) {
                for (LogicQueueMappingItem item: items) {
                    if (item.getStartOffset() != 0) {
                        throw new RuntimeException("The start offset dose not begin from 0");
                    }
                    TopicConfig topicConfig = brokerConfigMap.get(item.getBname());
                    if (topicConfig == null) {
                        throw new RuntimeException("The broker of item dose not exist");
                    }
                    if (item.getQueueId() >= topicConfig.getWriteQueueNums()) {
                        throw new RuntimeException("The physical queue id is overflow the write queues");
                    }
                }
            }
        }
    }



    public static Map<Integer, TopicQueueMappingOne> checkAndBuildMappingItems(List<TopicQueueMappingDetail> mappingDetailList, boolean replace, boolean checkConsistence) {
        mappingDetailList.sort((o1, o2) -> (int) (o2.getEpoch() - o1.getEpoch()));

        int maxNum = 0;
        Map<Integer, TopicQueueMappingOne> globalIdMap = new HashMap<>();
        for (TopicQueueMappingDetail mappingDetail : mappingDetailList) {
            if (mappingDetail.totalQueues > maxNum) {
                maxNum = mappingDetail.totalQueues;
            }
            for (Map.Entry<Integer, List<LogicQueueMappingItem>>  entry : mappingDetail.getHostedQueues().entrySet()) {
                Integer globalid = entry.getKey();
                checkLogicQueueMappingItemOffset(entry.getValue());
                String leaderBrokerName  = getLeaderBroker(entry.getValue());
                if (!leaderBrokerName.equals(mappingDetail.getBname())) {
                    //not the leader
                    continue;
                }
                if (globalIdMap.containsKey(globalid)) {
                    if (!replace) {
                        throw new RuntimeException(String.format("The queue id is duplicated in broker %s %s", leaderBrokerName, mappingDetail.getBname()));
                    }
                } else {
                    globalIdMap.put(globalid, new TopicQueueMappingOne(mappingDetail, mappingDetail.topic, mappingDetail.bname, globalid, entry.getValue()));
                }
            }
        }
        if (checkConsistence) {
            if (maxNum != globalIdMap.size()) {
                throw new RuntimeException(String.format("The total queue number in config dose not match the real hosted queues %d != %d", maxNum, globalIdMap.size()));
            }
            for (int i = 0; i < maxNum; i++) {
                if (!globalIdMap.containsKey(i)) {
                    throw new RuntimeException(String.format("The queue number %s is not in globalIdMap", i));
                }
            }
        }
        checkIfReusePhysicalQueue(globalIdMap.values());
        return globalIdMap;
    }

    public static String getLeaderBroker(List<LogicQueueMappingItem> items) {
        return getLeaderItem(items).getBname();
    }
    public static LogicQueueMappingItem getLeaderItem(List<LogicQueueMappingItem> items) {
        assert items.size() > 0;
        return items.get(items.size() - 1);
    }

    public static String writeToTemp(TopicRemappingDetailWrapper wrapper, boolean after) {
        String topic = wrapper.getTopic();
        String data = wrapper.toJson();
        String suffix = TopicRemappingDetailWrapper.SUFFIX_BEFORE;
        if (after) {
            suffix = TopicRemappingDetailWrapper.SUFFIX_AFTER;
        }
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + topic + "-" + wrapper.getEpoch() + suffix;
        try {
            MixAll.string2File(data, fileName);
            return fileName;
        } catch (Exception e) {
            throw new RuntimeException("write file failed " + fileName,e);
        }
    }

    public static long blockSeqRoundUp(long offset, long blockSeqSize) {
        long num = offset / blockSeqSize;
        long left = offset % blockSeqSize;
        if (left < blockSeqSize / 2) {
            return (num + 1) * blockSeqSize;
        } else {
            return (num + 2) * blockSeqSize;
        }
    }

    public static void checkTargetBrokersComplete(Set<String> targetBrokers, Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        for (String broker : brokerConfigMap.keySet()) {
            if (brokerConfigMap.get(broker).getMappingDetail().getHostedQueues().isEmpty()) {
                continue;
            }
            if (!targetBrokers.contains(broker)) {
                throw new RuntimeException("The existed broker " + broker + " dose not in target brokers ");
            }
        }
    }

    public static void checkNonTargetBrokers(Set<String> targetBrokers, Set<String> nonTargetBrokers) {
        for (String broker : nonTargetBrokers) {
            if (targetBrokers.contains(broker)) {
                throw new RuntimeException("The non-target broker exist in target broker");
            }
        }
    }

    public static TopicRemappingDetailWrapper createTopicConfigMapping(String topic, int queueNum, Set<String> targetBrokers, Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        checkTargetBrokersComplete(targetBrokers, brokerConfigMap);
        Map<Integer, TopicQueueMappingOne> globalIdMap = new HashMap<>();
        Map.Entry<Long, Integer> maxEpochAndNum = new AbstractMap.SimpleImmutableEntry<>(System.currentTimeMillis(), queueNum);
        if (!brokerConfigMap.isEmpty()) {
            maxEpochAndNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            checkIfReusePhysicalQueue(globalIdMap.values());
            checkPhysicalQueueConsistence(brokerConfigMap);
        }
        if (queueNum < globalIdMap.size()) {
            throw new RuntimeException(String.format("Cannot decrease the queue num for static topic %d < %d", queueNum, globalIdMap.size()));
        }
        //check the queue number
        if (queueNum == globalIdMap.size()) {
            throw new RuntimeException("The topic queue num is equal the existed queue num, do nothing");
        }

        //the check is ok, now do the mapping allocation
        Map<String, Integer> brokerNumMap = new HashMap<>();
        for (String broker: targetBrokers) {
            brokerNumMap.put(broker, 0);
        }
        final Map<Integer, String> oldIdToBroker = new HashMap<>();
        for (Map.Entry<Integer, TopicQueueMappingOne> entry : globalIdMap.entrySet()) {
            String leaderbroker = entry.getValue().getBname();
            oldIdToBroker.put(entry.getKey(), leaderbroker);
            if (!brokerNumMap.containsKey(leaderbroker)) {
                brokerNumMap.put(leaderbroker, 1);
            } else {
                brokerNumMap.put(leaderbroker, brokerNumMap.get(leaderbroker) + 1);
            }
        }
        TopicQueueMappingUtils.MappingAllocator allocator = TopicQueueMappingUtils.buildMappingAllocator(oldIdToBroker, brokerNumMap, null);
        allocator.upToNum(queueNum);
        Map<Integer, String> newIdToBroker = allocator.getIdToBroker();

        //construct the topic configAndMapping
        long newEpoch = Math.max(maxEpochAndNum.getKey() + 1000, System.currentTimeMillis());
        for (Map.Entry<Integer, String> e : newIdToBroker.entrySet()) {
            Integer queueId = e.getKey();
            String broker = e.getValue();
            if (globalIdMap.containsKey(queueId)) {
                //ignore the exited
                continue;
            }
            TopicConfigAndQueueMapping configMapping;
            if (!brokerConfigMap.containsKey(broker)) {
                configMapping = new TopicConfigAndQueueMapping(new TopicConfig(topic), new TopicQueueMappingDetail(topic, 0, broker, System.currentTimeMillis()));
                configMapping.setWriteQueueNums(1);
                configMapping.setReadQueueNums(1);
                brokerConfigMap.put(broker, configMapping);
            } else {
                configMapping = brokerConfigMap.get(broker);
                configMapping.setWriteQueueNums(configMapping.getWriteQueueNums() + 1);
                configMapping.setReadQueueNums(configMapping.getReadQueueNums() + 1);
            }
            LogicQueueMappingItem mappingItem = new LogicQueueMappingItem(0, configMapping.getWriteQueueNums() - 1, broker, 0, 0, -1, -1, -1);
            TopicQueueMappingDetail.putMappingInfo(configMapping.getMappingDetail(), queueId, new ArrayList<>(Collections.singletonList(mappingItem)));
        }

        // set the topic config
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            configMapping.getMappingDetail().setEpoch(newEpoch);
            configMapping.getMappingDetail().setTotalQueues(queueNum);
        }
        //double check the config
        {
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
            checkIfReusePhysicalQueue(globalIdMap.values());
            checkPhysicalQueueConsistence(brokerConfigMap);
        }
        return new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_CREATE_OR_UPDATE, newEpoch, brokerConfigMap, new HashSet<>(), new HashSet<>());
    }


    public static TopicRemappingDetailWrapper remappingStaticTopic(String topic, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, Set<String> targetBrokers) {
        Map.Entry<Long, Integer> maxEpochAndNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
        TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
        TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());

        //the check is ok, now do the mapping allocation
        int maxNum = maxEpochAndNum.getValue();

        Map<String, Integer> brokerNumMap = new HashMap<>();
        for (String broker: targetBrokers) {
            brokerNumMap.put(broker, 0);
        }
        Map<String, Integer> brokerNumMapBeforeRemapping = new HashMap<>();
        for (TopicQueueMappingOne mappingOne: globalIdMap.values()) {
            if (brokerNumMapBeforeRemapping.containsKey(mappingOne.bname)) {
                brokerNumMapBeforeRemapping.put(mappingOne.bname, brokerNumMapBeforeRemapping.get(mappingOne.bname) + 1);
            } else {
                brokerNumMapBeforeRemapping.put(mappingOne.bname, 1);
            }
        }

        TopicQueueMappingUtils.MappingAllocator allocator = TopicQueueMappingUtils.buildMappingAllocator(new HashMap<>(), brokerNumMap, brokerNumMapBeforeRemapping);
        allocator.upToNum(maxNum);
        Map<String, Integer> expectedBrokerNumMap = allocator.getBrokerNumMap();
        Queue<Integer> waitAssignQueues = new ArrayDeque<>();
        //cannot directly use the idBrokerMap from allocator, for the number of globalId maybe not in the natural order
        Map<Integer, String> expectedIdToBroker = new HashMap<>();
        //the following logic will make sure that, for one broker, either "map in" or "map out"
        //It can't both,  map in some queues but also map out some queues.
        for (Map.Entry<Integer, TopicQueueMappingOne> entry : globalIdMap.entrySet()) {
            Integer queueId = entry.getKey();
            TopicQueueMappingOne mappingOne = entry.getValue();
            String leaderBroker = mappingOne.getBname();
            if (expectedBrokerNumMap.containsKey(leaderBroker)) {
                if (expectedBrokerNumMap.get(leaderBroker) > 0) {
                    expectedIdToBroker.put(queueId, leaderBroker);
                    expectedBrokerNumMap.put(leaderBroker, expectedBrokerNumMap.get(leaderBroker) - 1);
                } else {
                    waitAssignQueues.add(queueId);
                    expectedBrokerNumMap.remove(leaderBroker);
                }
            } else {
                waitAssignQueues.add(queueId);
            }
        }

        for (Map.Entry<String, Integer> entry: expectedBrokerNumMap.entrySet()) {
            String broker = entry.getKey();
            Integer queueNum = entry.getValue();
            for (int i = 0; i < queueNum; i++) {
                Integer queueId = waitAssignQueues.poll();
                assert queueId != null;
                expectedIdToBroker.put(queueId, broker);
            }
        }
        long newEpoch = Math.max(maxEpochAndNum.getKey() + 1000, System.currentTimeMillis());

        //Now construct the remapping info
        Set<String> brokersToMapOut = new HashSet<>();
        Set<String> brokersToMapIn = new HashSet<>();
        for (Map.Entry<Integer, String> mapEntry : expectedIdToBroker.entrySet()) {
            Integer queueId = mapEntry.getKey();
            String broker = mapEntry.getValue();
            TopicQueueMappingOne topicQueueMappingOne = globalIdMap.get(queueId);
            assert topicQueueMappingOne != null;
            if (topicQueueMappingOne.getBname().equals(broker)) {
                continue;
            }
            //remapping
            final String mapInBroker = broker;
            final String mapOutBroker = topicQueueMappingOne.getBname();
            brokersToMapIn.add(mapInBroker);
            brokersToMapOut.add(mapOutBroker);
            TopicConfigAndQueueMapping mapInConfig = brokerConfigMap.get(mapInBroker);
            TopicConfigAndQueueMapping mapOutConfig = brokerConfigMap.get(mapOutBroker);

            if (mapInConfig == null) {
                mapInConfig = new TopicConfigAndQueueMapping(new TopicConfig(topic, 0, 0), new TopicQueueMappingDetail(topic, maxNum, mapInBroker, newEpoch));
                brokerConfigMap.put(mapInBroker, mapInConfig);
            }

            mapInConfig.setWriteQueueNums(mapInConfig.getWriteQueueNums() + 1);
            mapInConfig.setReadQueueNums(mapInConfig.getReadQueueNums() + 1);

            List<LogicQueueMappingItem> items = new ArrayList<>(topicQueueMappingOne.getItems());
            LogicQueueMappingItem last = items.get(items.size() - 1);
            items.add(new LogicQueueMappingItem(last.getGen() + 1, mapInConfig.getWriteQueueNums() - 1, mapInBroker, -1, 0, -1, -1, -1));

            //Use the same object
            TopicQueueMappingDetail.putMappingInfo(mapInConfig.getMappingDetail(), queueId, items);
            TopicQueueMappingDetail.putMappingInfo(mapOutConfig.getMappingDetail(), queueId, items);
        }

        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            configMapping.getMappingDetail().setEpoch(newEpoch);
            configMapping.getMappingDetail().setTotalQueues(maxNum);
        }

        //double check
        {
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());
            TopicQueueMappingUtils.checkLeaderInTargetBrokers(globalIdMap.values(), targetBrokers);
        }
        return new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_REMAPPING, newEpoch, brokerConfigMap, brokersToMapIn, brokersToMapOut);
    }

    public static LogicQueueMappingItem findLogicQueueMappingItem(List<LogicQueueMappingItem> mappingItems, long logicOffset, boolean ignoreNegative) {
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return null;
        }
        //Could use bi-search to polish performance
        for (int i = mappingItems.size() - 1; i >= 0; i--) {
            LogicQueueMappingItem item =  mappingItems.get(i);
            if (ignoreNegative && item.getLogicOffset() < 0) {
                continue;
            }
            if (logicOffset >= item.getLogicOffset()) {
                return item;
            }
        }
        //if not found, maybe out of range, return the first one
        for (int i = 0; i < mappingItems.size(); i++) {
            LogicQueueMappingItem item =  mappingItems.get(i);
            if (ignoreNegative && item.getLogicOffset() < 0) {
                continue;
            } else {
                return item;
            }
        }
        return null;
    }

    public static LogicQueueMappingItem findNext(List<LogicQueueMappingItem> items, LogicQueueMappingItem currentItem, boolean ignoreNegative) {
        if (items == null
            || currentItem == null) {
            return null;
        }
        for (int i = 0; i < items.size(); i++) {
            LogicQueueMappingItem item = items.get(i);
            if (ignoreNegative && item.getLogicOffset() < 0) {
                continue;
            }
            if (item.getGen() == currentItem.getGen()) {
                if (i < items.size() - 1) {
                    item = items.get(i  + 1);
                    if (ignoreNegative && item.getLogicOffset() < 0) {
                        return null;
                    } else {
                        return item;
                    }
                } else {
                    return null;
                }
            }
        }
        return null;
    }


    public static boolean checkIfLeader(List<LogicQueueMappingItem> items, TopicQueueMappingDetail mappingDetail) {
        if (items == null
            || mappingDetail == null
            || items.isEmpty()) {
            return false;
        }
        return items.get(items.size() - 1).getBname().equals(mappingDetail.getBname());
    }
}
