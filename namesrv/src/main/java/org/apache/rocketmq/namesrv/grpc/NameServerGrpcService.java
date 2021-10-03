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

package org.apache.rocketmq.namesrv.grpc;

import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import com.google.common.net.HostAndPort;
import com.google.rpc.Code;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.grpc.common.Converter;
import org.apache.rocketmq.grpc.common.ResponseBuilder;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameServerGrpcService extends MessagingServiceGrpc.MessagingServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final NamesrvController controller;

    public NameServerGrpcService(NamesrvController controller) {
        this.controller = controller;
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        Resource topic = request.getTopic();
        String topicName = Converter.getResourceNameWithNamespace(topic);

        TopicRouteData routeData = controller.getRouteInfoManager().pickupTopicRouteData(topicName);
        if (routeData == null) {
            ResponseWriter.write(responseObserver, QueryRouteResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.NOT_FOUND, "topic not found"))
                .build());
        }

        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();

        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            Map<Long, Broker> brokerIdMap = new HashMap<>();
            String brokerName = brokerData.getBrokerName();
            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                HostAndPort hostAndPort = HostAndPort.fromString(entry.getValue());
                Broker broker = Broker.newBuilder()
                    .setName(brokerName)
                    .setId(Math.toIntExact(brokerId))
                    .setEndpoints(Endpoints.newBuilder()
                        .setScheme(AddressScheme.IPv4)
                        .setAddresses(
                            0,
                            Address.newBuilder()
                                .setPort(hostAndPort.getPort())
                                .setHost(hostAndPort.getHostText())
                        )
                        .build())
                    .build();

                brokerIdMap.put(brokerId, broker);
            }
            brokerMap.put(brokerName, brokerIdMap);
        }

        List<Partition> partitionList = new ArrayList<>();
        for (QueueData queueData : routeData.getQueueDatas()) {
            String brokerName = queueData.getBrokerName();
            Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
            if (brokerIdMap == null) {
                break;
            }
            for (Broker broker : brokerIdMap.values()) {
                partitionList.addAll(Converter.generatePartitionList(queueData, topic, broker));
            }
        }

        ResponseWriter.write(responseObserver, QueryRouteResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"))
            .addAllPartitions(partitionList)
            .build());
    }
}
