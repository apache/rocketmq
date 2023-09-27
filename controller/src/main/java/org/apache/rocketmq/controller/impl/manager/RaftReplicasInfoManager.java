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
package org.apache.rocketmq.controller.impl.manager;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelResponse;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerResponse;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoResponse;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventResponse;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RaftReplicasInfoManager extends ReplicasInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final Map<BrokerIdentityInfo/* brokerIdentity*/, BrokerLiveInfo> brokerLiveTable = new ConcurrentHashMap<>(256);

    public RaftReplicasInfoManager(ControllerConfig controllerConfig) {
        super(controllerConfig);
    }

    public ControllerResult<GetBrokerLiveInfoResponse> getBrokerLiveInfo(final GetBrokerLiveInfoRequest request) {
        BrokerIdentityInfo brokerIdentityInfo = request.getBrokerIdentity();
        ControllerResult<GetBrokerLiveInfoResponse> result = new ControllerResult<>(new GetBrokerLiveInfoResponse());
        Map<BrokerIdentityInfo/* brokerIdentity*/, BrokerLiveInfo> resBrokerLiveTable = new HashMap<>();
        if (brokerIdentityInfo == null || brokerIdentityInfo.isEmpty()) {
            resBrokerLiveTable.putAll(this.brokerLiveTable);
        } else {
            if (brokerLiveTable.containsKey(brokerIdentityInfo)) {
                resBrokerLiveTable.put(brokerIdentityInfo, brokerLiveTable.get(brokerIdentityInfo));
            } else {
                log.warn("GetBrokerLiveInfo failed, brokerIdentityInfo: {} not exist", brokerIdentityInfo);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS, "brokerIdentityInfo not exist");
            }
        }
        try {
            result.setBody(JSON.toJSONBytes(resBrokerLiveTable));
        } catch (Throwable e) {
            log.error("json serialize resBrokerLiveTable {} error", resBrokerLiveTable, e);
            result.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "serialize error");
        }

        return result;
    }

    public ControllerResult<RaftBrokerHeartBeatEventResponse> onBrokerHeartBeat(
        RaftBrokerHeartBeatEventRequest request) {
        BrokerIdentityInfo brokerIdentityInfo = request.getBrokerIdentityInfo();
        BrokerLiveInfo brokerLiveInfo = request.getBrokerLiveInfo();
        ControllerResult<RaftBrokerHeartBeatEventResponse> result = new ControllerResult<>(new RaftBrokerHeartBeatEventResponse());
        BrokerLiveInfo prev = brokerLiveTable.computeIfAbsent(brokerIdentityInfo, identityInfo -> {
            log.info("new broker registered, brokerIdentityInfo: {}", identityInfo);
            return brokerLiveInfo;
        });
        prev.setLastUpdateTimestamp(brokerLiveInfo.getLastUpdateTimestamp());
        prev.setHeartbeatTimeoutMillis(brokerLiveInfo.getHeartbeatTimeoutMillis());
        prev.setElectionPriority(brokerLiveInfo.getElectionPriority());
        if (brokerLiveInfo.getEpoch() > prev.getEpoch() || brokerLiveInfo.getEpoch() == prev.getEpoch() && brokerLiveInfo.getMaxOffset() > prev.getMaxOffset()) {
            prev.setEpoch(brokerLiveInfo.getEpoch());
            prev.setMaxOffset(brokerLiveInfo.getMaxOffset());
            prev.setConfirmOffset(brokerLiveInfo.getConfirmOffset());
        }
        return result;
    }

    public ControllerResult<BrokerCloseChannelResponse> onBrokerCloseChannel(BrokerCloseChannelRequest request) {
        BrokerIdentityInfo brokerIdentityInfo = request.getBrokerIdentityInfo();
        ControllerResult<BrokerCloseChannelResponse> result = new ControllerResult<>(new BrokerCloseChannelResponse());
        if (brokerIdentityInfo == null || brokerIdentityInfo.isEmpty()) {
            log.warn("onBrokerCloseChannel failed, brokerIdentityInfo is null");
        } else {
            brokerLiveTable.remove(brokerIdentityInfo);
            log.info("onBrokerCloseChannel success, brokerIdentityInfo: {}", brokerIdentityInfo);
        }
        return result;
    }

    public ControllerResult<CheckNotActiveBrokerResponse> checkNotActiveBroker(CheckNotActiveBrokerRequest request) {
        List<BrokerIdentityInfo> notActiveBrokerIdentityInfoList = new ArrayList<>();
        long checkTime = request.getCheckTimeMillis();
        final Iterator<Map.Entry<BrokerIdentityInfo, BrokerLiveInfo>> iterator = this.brokerLiveTable.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> next = iterator.next();
            long last = next.getValue().getLastUpdateTimestamp();
            long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
            if (checkTime - last > timeoutMillis) {
                notActiveBrokerIdentityInfoList.add(next.getKey());
                iterator.remove();
                log.warn("Broker expired, brokerInfo {}, expired {}ms", next.getKey(), timeoutMillis);
            }
        }
        List<String> needReElectBrokerNames = scanNeedReelectBrokerSets(new BrokerValidPredicate() {
            @Override
            public boolean check(String clusterName, String brokerName, Long brokerId) {
                return !isBrokerActive(clusterName, brokerName, brokerId, checkTime);
            }
        });
        Set<String> alreadyReportedBrokerName = notActiveBrokerIdentityInfoList.stream()
            .map(BrokerIdentityInfo::getBrokerName)
            .collect(Collectors.toSet());
        notActiveBrokerIdentityInfoList.addAll(needReElectBrokerNames.stream()
            .filter(brokerName -> !alreadyReportedBrokerName.contains(brokerName))
            .map(brokerName -> new BrokerIdentityInfo(null, brokerName, null))
            .collect(Collectors.toList()));
        ControllerResult<CheckNotActiveBrokerResponse> result = new ControllerResult<>(new CheckNotActiveBrokerResponse());
        try {
            result.setBody(JSON.toJSONBytes(notActiveBrokerIdentityInfoList));
        } catch (Throwable e) {
            log.error("json serialize notActiveBrokerIdentityInfoList {} error", notActiveBrokerIdentityInfoList, e);
            result.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "serialize error");
        }
        return result;
    }

    public boolean isBrokerActive(String clusterName, String brokerName, Long brokerId, long invokeTime) {
        final BrokerLiveInfo info = this.brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
        if (info != null) {
            long last = info.getLastUpdateTimestamp();
            long timeoutMillis = info.getHeartbeatTimeoutMillis();
            return (last + timeoutMillis) >= invokeTime;
        }
        return false;
    }

    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId) {
        return this.brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
    }

    @Override
    public byte[] serialize() throws Throwable {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            final byte[] superSerialize = super.serialize();
            putInt(outputStream, superSerialize.length);
            outputStream.write(superSerialize);
            putInt(outputStream, this.brokerLiveTable.size());
            for (Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> entry : brokerLiveTable.entrySet()) {
                final byte[] brokerIdentityInfo = hessianSerialize(entry.getKey());
                final byte[] brokerLiveInfo = hessianSerialize(entry.getValue());
                putInt(outputStream, brokerIdentityInfo.length);
                outputStream.write(brokerIdentityInfo);
                putInt(outputStream, brokerLiveInfo.length);
                outputStream.write(brokerLiveInfo);
            }
            return outputStream.toByteArray();
        } catch (Throwable e) {
            log.error("serialize replicaInfoTable or syncStateSetInfoTable error", e);
            throw e;
        }
    }

    @Override
    public void deserializeFrom(byte[] data) throws Throwable {
        int index = 0;
        this.brokerLiveTable.clear();

        try {
            int superTableSize = getInt(data, index);
            index += 4;
            byte[] superTableData = new byte[superTableSize];
            System.arraycopy(data, index, superTableData, 0, superTableSize);
            super.deserializeFrom(superTableData);
            index += superTableSize;
            int brokerLiveTableSize = getInt(data, index);
            index += 4;
            for (int i = 0; i < brokerLiveTableSize; i++) {
                int brokerIdentityInfoLength = getInt(data, index);
                index += 4;
                byte[] brokerIdentityInfoArray = new byte[brokerIdentityInfoLength];
                System.arraycopy(data, index, brokerIdentityInfoArray, 0, brokerIdentityInfoLength);
                BrokerIdentityInfo brokerIdentityInfo = (BrokerIdentityInfo) hessianDeserialize(brokerIdentityInfoArray);
                index += brokerIdentityInfoLength;
                int brokerLiveInfoLength = getInt(data, index);
                index += 4;
                byte[] brokerLiveInfoArray = new byte[brokerLiveInfoLength];
                System.arraycopy(data, index, brokerLiveInfoArray, 0, brokerLiveInfoLength);
                BrokerLiveInfo brokerLiveInfo = (BrokerLiveInfo) hessianDeserialize(brokerLiveInfoArray);
                index += brokerLiveInfoLength;
                this.brokerLiveTable.put(brokerIdentityInfo, brokerLiveInfo);
            }
        } catch (Throwable e) {
            log.error("deserialize replicaInfoTable or syncStateSetInfoTable error", e);
            throw e;
        }
    }

    public static class BrokerValidPredicateWithInvokeTime implements BrokerValidPredicate {
        private final long invokeTime;
        private final RaftReplicasInfoManager raftBrokerHeartBeatManager;

        public BrokerValidPredicateWithInvokeTime(long invokeTime, RaftReplicasInfoManager raftBrokerHeartBeatManager) {
            this.invokeTime = invokeTime;
            this.raftBrokerHeartBeatManager = raftBrokerHeartBeatManager;
        }

        @Override
        public boolean check(String clusterName, String brokerName, Long brokerId) {
            return raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId, invokeTime);
        }
    }
}
