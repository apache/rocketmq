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

package org.apache.rocketmq.proxy.common;

import java.util.Map;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;

public class SystemResourceAwareRpcHook implements RPCHook {
    private final RPCHook userHook;
    private final RPCHook systemHook;

    public SystemResourceAwareRpcHook(RPCHook userHook, RPCHook systemHook) {
        this.userHook = userHook;
        this.systemHook = systemHook;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (isTargetingSystemResource(request)) {
            if (systemHook != null) {
                systemHook.doBeforeRequest(remoteAddr, request);
            }
            return;
        }
        if (userHook != null) {
            userHook.doBeforeRequest(remoteAddr, request);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        if (isTargetingSystemResource(request)) {
            if (systemHook != null) {
                systemHook.doAfterResponse(remoteAddr, request, response);
            }
            return;
        }
        if (userHook != null) {
            userHook.doAfterResponse(remoteAddr, request, response);
        }
    }

    private boolean isTargetingSystemResource(RemotingCommand request) {
        if (!InternalContextHolder.isInternalScope()) {
            return false;
        }

        if (request == null) {
            return false;
        }

        int code = request.getCode();
        try {
            switch (code) {
                case RequestCode.GET_ROUTEINFO_BY_TOPIC:
                    GetRouteInfoRequestHeader routeHeader = request
                        .decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
                    return TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(routeHeader.getTopic());

                case RequestCode.SEND_MESSAGE:
                    SendMessageRequestHeader sendHeader = request
                        .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                    return TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(sendHeader.getTopic())
                        || MixAll.CLIENT_INNER_PRODUCER_GROUP.equals(sendHeader.getProducerGroup());

                case RequestCode.SEND_MESSAGE_V2:
                    SendMessageRequestHeaderV2 sendHeaderV2 = request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
                    SendMessageRequestHeader v1Header =
                        SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(sendHeaderV2);
                    return TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(v1Header.getTopic())
                        || MixAll.CLIENT_INNER_PRODUCER_GROUP.equals(v1Header.getProducerGroup());

                case RequestCode.UNREGISTER_CLIENT:
                    UnregisterClientRequestHeader unregisterHeader = request
                        .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    return MixAll.CLIENT_INNER_PRODUCER_GROUP.equals(unregisterHeader.getProducerGroup())
                        || MixAll.TOOLS_CONSUMER_GROUP.equals(unregisterHeader.getConsumerGroup());

                default:
                    return checkFallbackExtFields(request.getExtFields());
            }
        } catch (Exception e) {
            return false;
        }
    }

    private boolean checkFallbackExtFields(Map<String, String> extFields) {
        if (extFields == null || extFields.isEmpty()) {
            return false;
        }
        String topic = extFields.get("topic");
        if (TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(topic)) {
            return true;
        }

        String producerGroup = extFields.get("producerGroup");
        if (MixAll.CLIENT_INNER_PRODUCER_GROUP.equals(producerGroup)) {
            return true;
        }

        String consumerGroup = extFields.get("consumerGroup");
        if (MixAll.TOOLS_CONSUMER_GROUP.equals(consumerGroup)) {
            return true;
        }

        String generalGroup = extFields.get("group");
        return MixAll.CLIENT_INNER_PRODUCER_GROUP.equals(generalGroup);
    }
}
