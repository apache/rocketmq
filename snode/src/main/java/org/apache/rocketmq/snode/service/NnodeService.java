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
package org.apache.rocketmq.snode.service;

import java.util.Set;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.snode.config.SnodeConfig;

public interface NnodeService {
    void registerSnode(SnodeConfig snodeConfig);

    void updateNnodeAddressList(final String addrs);

    String fetchNnodeAdress();

    void updateTopicRouteDataByTopic();

    Set<String> getEnodeClusterInfo(String clusterName);

    ClusterInfo updateEnodeClusterInfo() throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException;

    String getAddressByEnodeName(String brokerName,
        boolean isUseSlave) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException;

    TopicRouteData getTopicRouteDataByTopic(String topic,
        boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;
}
