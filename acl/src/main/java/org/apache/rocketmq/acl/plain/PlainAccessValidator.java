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
package org.apache.rocketmq.acl.plain;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import com.google.protobuf.GeneratedMessageV3;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.codec.DecoderException;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.AuthorizationHeader;
import org.apache.rocketmq.acl.common.MetadataHeader;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.acl.plain.PlainAccessResource.getRetryTopic;

public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionManager aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionManager();
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        PlainAccessResource accessResource = new PlainAccessResource();
        if (remoteAddr != null && remoteAddr.contains(":")) {
            accessResource.setWhiteRemoteAddress(remoteAddr.substring(0, remoteAddr.lastIndexOf(':')));
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddr);
        }

        accessResource.setRequestCode(request.getCode());

        if (request.getExtFields() == null) {
            // If request's extFields is null,then return accessResource directly(users can use whiteAddress pattern)
            // The following logic codes depend on the request's extFields not to be null.
            return accessResource;
        }
        accessResource.setAccessKey(request.getExtFields().get(SessionCredentials.ACCESS_KEY));
        accessResource.setSignature(request.getExtFields().get(SessionCredentials.SIGNATURE));
        accessResource.setSecretToken(request.getExtFields().get(SessionCredentials.SECURITY_TOKEN));

        try {
            switch (request.getCode()) {
                case RequestCode.SEND_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.PUB);
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                    accessResource.addResourceAndPerm(request.getExtFields().get("b"), Permission.PUB);
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    accessResource.addResourceAndPerm(request.getExtFields().get("originTopic"), Permission.PUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("group")), Permission.SUB);
                    break;
                case RequestCode.PULL_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("consumerGroup")), Permission.SUB);
                    break;
                case RequestCode.QUERY_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        accessResource.addResourceAndPerm(getRetryTopic(data.getGroupName()), Permission.SUB);
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            accessResource.addResourceAndPerm(subscriptionData.getTopic(), Permission.SUB);
                        }
                    }
                    break;
                case RequestCode.UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader unregisterClientRequestHeader =
                        (UnregisterClientRequestHeader) request
                            .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(unregisterClientRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader =
                        (GetConsumerListByGroupRequestHeader) request
                            .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(getConsumerListByGroupRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        (UpdateConsumerOffsetRequestHeader) request
                            .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(updateConsumerOffsetRequestHeader.getConsumerGroup()), Permission.SUB);
                    accessResource.addResourceAndPerm(updateConsumerOffsetRequestHeader.getTopic(), Permission.SUB);
                    break;
                default:
                    break;

            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }

        // Content
        SortedMap<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : request.getExtFields().entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())
                && !MixAll.UNIQUE_MSG_QUERY_FLAG.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        accessResource.setContent(AclUtils.combineRequestContent(request, map));
        return accessResource;
    }

    @Override public AccessResource parse(GeneratedMessageV3 messageV3, MetadataHeader header) {
        PlainAccessResource accessResource = new PlainAccessResource();
        String remoteAddress = header.getRemoteAddress();
        if (remoteAddress != null && remoteAddress.contains(":")) {
            accessResource.setWhiteRemoteAddress(RemotingHelper.parseHostFromAddress(remoteAddress));
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddress);
        }
        try {
            AuthorizationHeader authorizationHeader = new AuthorizationHeader(header.getAuthorization());
            accessResource.setAccessKey(authorizationHeader.getAccessKey());
            accessResource.setSignature(authorizationHeader.getSignature());
        } catch (DecoderException e) {
            throw new AclException(e.getMessage(), e);
        }
        accessResource.setSecretToken(header.getSessionToken());
        accessResource.setRequestCode(header.getRequestCode());
        accessResource.setContent(header.getDatetime().getBytes(StandardCharsets.UTF_8));

        try {
            String rpcFullName = messageV3.getDescriptorForType().getFullName();
            if (HeartbeatRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                HeartbeatRequest request = (HeartbeatRequest) messageV3;
                if (request.hasGroup()) {
                    Resource group = request.getGroup();
                    String groupName = NamespaceUtil.wrapNamespace(group.getResourceNamespace(), group.getName());
                    accessResource.addResourceAndPerm(groupName, Permission.SUB);
                }
            } else if (SendMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                SendMessageRequest request = (SendMessageRequest) messageV3;
                if (request.getMessagesCount() <= 0) {
                    throw new AclException("SendMessageRequest, messageCount is zero", ResponseCode.MESSAGE_ILLEGAL);
                }
                Resource topic = request.getMessages(0).getTopic();
                for (Message message : request.getMessagesList()) {
                    if (!message.getTopic().equals(topic)) {
                        throw new AclException("SendMessageRequest, messages' topic is not consistent", ResponseCode.MESSAGE_ILLEGAL);
                    }
                }
                String topicName = NamespaceUtil.wrapNamespace(topic.getResourceNamespace(), topic.getName());
                accessResource.addResourceAndPerm(topicName, Permission.PUB);
            } else if (ReceiveMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                ReceiveMessageRequest request = (ReceiveMessageRequest) messageV3;
                Resource group = request.getGroup();
                String groupName = NamespaceUtil.wrapNamespace(group.getResourceNamespace(), group.getName());
                accessResource.addResourceAndPerm(groupName, Permission.SUB);
                Resource topic = request.getMessageQueue().getTopic();
                String topicName = NamespaceUtil.wrapNamespace(topic.getResourceNamespace(), topic.getName());
                accessResource.addResourceAndPerm(topicName, Permission.SUB);
            } else if (AckMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                AckMessageRequest request = (AckMessageRequest) messageV3;
                Resource group = request.getGroup();
                String groupName = NamespaceUtil.wrapNamespace(group.getResourceNamespace(), group.getName());
                accessResource.addResourceAndPerm(groupName, Permission.SUB);
                Resource topic = request.getTopic();
                String topicName = NamespaceUtil.wrapNamespace(topic.getResourceNamespace(), topic.getName());
                accessResource.addResourceAndPerm(topicName, Permission.SUB);
            } else if (ForwardMessageToDeadLetterQueueRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                ForwardMessageToDeadLetterQueueRequest request = (ForwardMessageToDeadLetterQueueRequest) messageV3;
                Resource group = request.getGroup();
                String groupName = NamespaceUtil.wrapNamespace(group.getResourceNamespace(), group.getName());
                accessResource.addResourceAndPerm(groupName, Permission.SUB);
                Resource topic = request.getTopic();
                String topicName = NamespaceUtil.wrapNamespace(topic.getResourceNamespace(), topic.getName());
                accessResource.addResourceAndPerm(topicName, Permission.SUB);
            } else if (EndTransactionRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                EndTransactionRequest request = (EndTransactionRequest) messageV3;
                Resource topic = request.getTopic();
                String topicName = NamespaceUtil.wrapNamespace(topic.getResourceNamespace(), topic.getName());
                accessResource.addResourceAndPerm(topicName, Permission.PUB);
            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }
        return accessResource;
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return aclPlugEngine.updateAccessConfig(plainAccessConfig);
    }

    @Override
    public boolean deleteAccessConfig(String accesskey) {
        return aclPlugEngine.deleteAccessConfig(accesskey);
    }

    @Override public String  getAclConfigVersion() {
        return aclPlugEngine.getAclConfigDataVersion();
    }

    @Override public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);
    }

    @Override public AclConfig getAllAclConfig() {
        return aclPlugEngine.getAllAclConfig();
    }
    
    @Override
    public Map<String, DataVersion> getAllAclConfigVersion() {
        return aclPlugEngine.getDataVersionMap();
    }
}
