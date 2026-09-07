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
package org.apache.rocketmq.auth.authorization.builder;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.body.CreateTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.DeleteSubscriptionGroupListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.DeleteTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class DefaultAuthorizationContextBuilder implements AuthorizationContextBuilder {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final String B = "b";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private final AuthConfig authConfig;
    private final RequestHeaderRegistry requestHeaderRegistry;

    public DefaultAuthorizationContextBuilder(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.requestHeaderRegistry = RequestHeaderRegistry.getInstance();
    }

    @Override
    public List<DefaultAuthorizationContext> build(Metadata metadata, GeneratedMessageV3 message) {
        List<DefaultAuthorizationContext> result = null;
        if (message instanceof SendMessageRequest) {
            SendMessageRequest request = (SendMessageRequest) message;
            if (request.getMessagesCount() <= 0) {
                throw new AuthorizationException("message is null.");
            }
            result = newPubContext(metadata, request.getMessages(0).getTopic());
        }
        if (message instanceof RecallMessageRequest) {
            RecallMessageRequest request = (RecallMessageRequest) message;
            result = newPubContext(metadata, request.getTopic());
        }
        if (message instanceof EndTransactionRequest) {
            EndTransactionRequest request = (EndTransactionRequest) message;
            result = newPubContext(metadata, request.getTopic());
        }
        if (message instanceof HeartbeatRequest) {
            HeartbeatRequest request = (HeartbeatRequest) message;
            if (StringUtils.isNotBlank(request.getGroup().getName())) {
                if (request.getClientType() == ClientType.PRODUCER) {
                    throw new AuthorizationException("group is not allowed for producer heartbeat.");
                }
                result = newGroupSubContexts(metadata, request.getGroup());
            }
        }
        if (message instanceof ReceiveMessageRequest) {
            ReceiveMessageRequest request = (ReceiveMessageRequest) message;
            if (!request.hasMessageQueue()) {
                throw new AuthorizationException("messageQueue is null.");
            }
            result = newSubContexts(metadata, request.getGroup(), request.getMessageQueue().getTopic());
        }
        if (message instanceof SyncLiteSubscriptionRequest) {
            SyncLiteSubscriptionRequest request = (SyncLiteSubscriptionRequest) message;
            result = newSubContexts(metadata, request.getGroup(), request.getTopic());
        }
        if (message instanceof AckMessageRequest) {
            AckMessageRequest request = (AckMessageRequest) message;
            result = newSubContexts(metadata, request.getGroup(), request.getTopic());
        }
        if (message instanceof ForwardMessageToDeadLetterQueueRequest) {
            ForwardMessageToDeadLetterQueueRequest request = (ForwardMessageToDeadLetterQueueRequest) message;
            result = newSubContexts(metadata, request.getGroup(), request.getTopic());
        }
        if (message instanceof NotifyClientTerminationRequest) {
            NotifyClientTerminationRequest request = (NotifyClientTerminationRequest) message;
            if (StringUtils.isNotBlank(request.getGroup().getName())) {
                result = newGroupSubContexts(metadata, request.getGroup());
            }
        }
        if (message instanceof ChangeInvisibleDurationRequest) {
            ChangeInvisibleDurationRequest request = (ChangeInvisibleDurationRequest) message;
            result = newSubContexts(metadata, request.getGroup(), request.getTopic());
        }
        if (message instanceof QueryRouteRequest) {
            QueryRouteRequest request = (QueryRouteRequest) message;
            result = newContext(metadata, request);
        }
        if (message instanceof QueryAssignmentRequest) {
            QueryAssignmentRequest request = (QueryAssignmentRequest) message;
            result = newSubContexts(metadata, request.getGroup(), request.getTopic());
        }
        if (message instanceof TelemetryCommand) {
            TelemetryCommand request = (TelemetryCommand) message;
            result = newContext(metadata, request);
        }
        if (CollectionUtils.isNotEmpty(result)) {
            result.forEach(context -> {
                context.setChannelId(metadata.get(GrpcConstants.CHANNEL_ID));
                context.setRpcCode(message.getDescriptorForType().getFullName());
            });
        }
        return result;
    }

    @Override
    public List<DefaultAuthorizationContext> build(ChannelHandlerContext context, RemotingCommand command) {
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        try {
            HashMap<String, String> fields = command.getExtFields();
            if (fields == null) {
                fields = new HashMap<>();
            }
            Subject subject = null;
            if (fields.containsKey(SessionCredentials.ACCESS_KEY)) {
                subject = User.of(fields.get(SessionCredentials.ACCESS_KEY));
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(context.channel());
            String sourceIp = StringUtils.substringBeforeLast(remoteAddr, CommonConstants.COLON);

            Resource topic;
            Resource group;
            switch (command.getCode()) {
                case RequestCode.GET_ROUTEINFO_BY_TOPIC:
                    String routeTopic = requireResource(fields.get(TOPIC), "topic");
                    if (NamespaceUtil.isRetryTopic(routeTopic)) {
                        group = Resource.ofGroup(routeTopic);
                        result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    } else {
                        topic = Resource.ofTopic(routeTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.PUB, Action.SUB, Action.GET), sourceIp));
                    }
                    break;
                case RequestCode.SEND_MESSAGE:
                    String sendTopic = requireResource(fields.get(TOPIC), "topic");
                    if (NamespaceUtil.isRetryTopic(sendTopic)) {
                        group = Resource.ofGroup(sendTopic);
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(sendTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                case RequestCode.SEND_BATCH_MESSAGE:
                    String compactSendTopic = requireResource(fields.get(B), "topic");
                    if (NamespaceUtil.isRetryTopic(compactSendTopic)) {
                        group = Resource.ofGroup(compactSendTopic);
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(compactSendTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.SEND_REPLY_MESSAGE:
                    topic = Resource.ofTopic(requireResource(fields.get(TOPIC), "topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    break;
                case RequestCode.SEND_REPLY_MESSAGE_V2:
                    topic = Resource.ofTopic(requireResource(fields.get(B), "topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    break;
                case RequestCode.RECALL_MESSAGE:
                    topic = Resource.ofTopic(requireResource(fields.get(TOPIC), "topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    break;
                case RequestCode.END_TRANSACTION:
                    if (StringUtils.isNotBlank(fields.get(TOPIC))) {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.VIEW_MESSAGE_BY_ID:
                    if (StringUtils.isNotBlank(fields.get(TOPIC))) {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.GET, sourceIp));
                    }
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    group = Resource.ofGroup(requireResource(fields.get(GROUP), "consumer group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.PULL_MESSAGE:
                case RequestCode.LITE_PULL_MESSAGE:
                    String pullTopic = requireResource(fields.get(TOPIC), "topic");
                    String pullGroup = requireResource(fields.get(CONSUMER_GROUP), "consumer group");
                    if (NamespaceUtil.isRetryTopic(pullTopic)) {
                        if (!StringUtils.equals(pullTopic, MixAll.getRetryTopic(pullGroup))) {
                            throw new AuthorizationException("retry topic does not match consumer group.");
                        }
                    } else {
                        topic = Resource.ofTopic(pullTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                    }
                    group = Resource.ofGroup(pullGroup);
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.BATCH_ACK_MESSAGE:
                    BatchAckMessageRequestBody batchAckBody = decodeRequiredBody(
                        command, BatchAckMessageRequestBody.class, "batch ack");
                    if (CollectionUtils.isEmpty(batchAckBody.getAcks())) {
                        throw new AuthorizationException("batch ack is empty.");
                    }
                    Set<String> ackResources = new LinkedHashSet<>();
                    for (BatchAck ack : batchAckBody.getAcks()) {
                        if (ack == null) {
                            throw new AuthorizationException("batch ack entry is null.");
                        }
                        addUniqueContext(result, ackResources, subject,
                            Resource.ofTopic(requireResource(ack.getTopic(), "topic")),
                            Action.SUB, sourceIp);
                        addUniqueContext(result, ackResources, subject,
                            Resource.ofGroup(requireResource(ack.getConsumerGroup(), "consumer group")),
                            Action.SUB, sourceIp);
                    }
                    break;
                case RequestCode.QUERY_ASSIGNMENT:
                    QueryAssignmentRequestBody assignmentBody = decodeRequiredBody(
                        command, QueryAssignmentRequestBody.class, "query assignment");
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofTopic(requireResource(assignmentBody.getTopic(), "topic")),
                        Action.SUB, sourceIp));
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofGroup(requireResource(assignmentBody.getConsumerGroup(), "consumer group")),
                        Action.SUB, sourceIp));
                    break;
                case RequestCode.SET_MESSAGE_REQUEST_MODE:
                    SetMessageRequestModeRequestBody modeBody = decodeRequiredBody(
                        command, SetMessageRequestModeRequestBody.class, "message request mode");
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofTopic(requireResource(modeBody.getTopic(), "topic")),
                        Action.SUB, sourceIp));
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofGroup(requireResource(modeBody.getConsumerGroup(), "consumer group")),
                        Action.UPDATE, sourceIp));
                    break;
                case RequestCode.CHECK_CLIENT_CONFIG:
                    CheckClientRequestBody checkClientBody = decodeRequiredBody(
                        command, CheckClientRequestBody.class, "client config");
                    if (checkClientBody.getSubscriptionData() == null) {
                        throw new AuthorizationException("subscription is null.");
                    }
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofTopic(requireResource(
                            checkClientBody.getSubscriptionData().getTopic(), "topic")),
                        Action.SUB, sourceIp));
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofGroup(requireResource(checkClientBody.getGroup(), "consumer group")),
                        Action.SUB, sourceIp));
                    break;
                case RequestCode.QUERY_MESSAGE:
                    topic = Resource.ofTopic(requireResource(fields.get(TOPIC), "topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = decodeRequiredBody(command, HeartbeatData.class, "heartbeat");
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        if (data == null) {
                            throw new AuthorizationException("consumer data is null.");
                        }
                        group = Resource.ofGroup(requireResource(data.getGroupName(), "consumer group"));
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            if (subscriptionData == null) {
                                throw new AuthorizationException("subscription is null.");
                            }
                            String subscriptionTopic =
                                requireResource(subscriptionData.getTopic(), "topic");
                            if (NamespaceUtil.isRetryTopic(subscriptionTopic)) {
                                continue;
                            }
                            topic = Resource.ofTopic(subscriptionTopic);
                            result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                        }
                    }
                    break;
                case RequestCode.UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader unregisterClientRequestHeader =
                        command.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    if (StringUtils.isNotBlank(unregisterClientRequestHeader.getConsumerGroup())) {
                        group = Resource.ofGroup(unregisterClientRequestHeader.getConsumerGroup());
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    }
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader =
                        command.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    group = Resource.ofGroup(requireResource(
                        getConsumerListByGroupRequestHeader.getConsumerGroup(), "consumer group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.QUERY_CONSUMER_OFFSET:
                    final QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader =
                        command.decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
                    String queryOffsetTopic = requireResource(
                        queryConsumerOffsetRequestHeader.getTopic(), "topic");
                    String queryOffsetGroup = requireResource(
                        queryConsumerOffsetRequestHeader.getConsumerGroup(), "consumer group");
                    if (!NamespaceUtil.isRetryTopic(queryOffsetTopic)) {
                        topic = Resource.ofTopic(queryOffsetTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    }
                    group = Resource.ofGroup(queryOffsetGroup);
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        command.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    String updateOffsetTopic = requireResource(
                        updateConsumerOffsetRequestHeader.getTopic(), "topic");
                    String updateOffsetGroup = requireResource(
                        updateConsumerOffsetRequestHeader.getConsumerGroup(), "consumer group");
                    if (!NamespaceUtil.isRetryTopic(updateOffsetTopic)) {
                        topic = Resource.ofTopic(updateOffsetTopic);
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    }
                    group = Resource.ofGroup(updateOffsetGroup);
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    break;
                case RequestCode.LOCK_BATCH_MQ:
                    LockBatchRequestBody lockBatchRequestBody = LockBatchRequestBody.decode(command.getBody(), LockBatchRequestBody.class);
                    group = Resource.ofGroup(requireResource(
                        lockBatchRequestBody.getConsumerGroup(), "consumer group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    if (CollectionUtils.isNotEmpty(lockBatchRequestBody.getMqSet())) {
                        for (MessageQueue messageQueue : lockBatchRequestBody.getMqSet()) {
                            String lockTopic = requireResource(messageQueue.getTopic(), "topic");
                            if (NamespaceUtil.isRetryTopic(lockTopic)) {
                                continue;
                            }
                            topic = Resource.ofTopic(lockTopic);
                            result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                        }
                    }
                    break;
                case RequestCode.UNLOCK_BATCH_MQ:
                    UnlockBatchRequestBody unlockBatchRequestBody = UnlockBatchRequestBody.decode(
                        command.getBody(), UnlockBatchRequestBody.class);
                    group = Resource.ofGroup(requireResource(
                        unlockBatchRequestBody.getConsumerGroup(), "consumer group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    if (CollectionUtils.isNotEmpty(unlockBatchRequestBody.getMqSet())) {
                        for (MessageQueue messageQueue : unlockBatchRequestBody.getMqSet()) {
                            String unlockTopic = requireResource(messageQueue.getTopic(), "topic");
                            if (NamespaceUtil.isRetryTopic(unlockTopic)) {
                                continue;
                            }
                            topic = Resource.ofTopic(unlockTopic);
                            result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                        }
                    }
                    break;
                case RequestCode.LITE_SUBSCRIPTION_CTL:
                    LiteSubscriptionCtlRequestBody liteSubscriptionBody = decodeRequiredBody(
                        command, LiteSubscriptionCtlRequestBody.class, "lite subscription");
                    if (CollectionUtils.isEmpty(liteSubscriptionBody.getSubscriptionSet())) {
                        throw new AuthorizationException("lite subscription is empty.");
                    }
                    Set<String> liteSubscriptionResources = new LinkedHashSet<>();
                    for (LiteSubscriptionDTO subscription : liteSubscriptionBody.getSubscriptionSet()) {
                        if (subscription == null) {
                            throw new AuthorizationException("lite subscription is null.");
                        }
                        addUniqueContext(result, liteSubscriptionResources, subject,
                            Resource.ofGroup(requireResource(subscription.getGroup(), "consumer group")),
                            Action.SUB, sourceIp);
                        addUniqueContext(result, liteSubscriptionResources, subject,
                            Resource.ofTopic(requireResource(subscription.getTopic(), "topic")),
                            Action.SUB, sourceIp);
                    }
                    break;
                case RequestCode.UPDATE_BROKER_CONFIG:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofCluster(authConfig.getClusterName()), Action.UPDATE, sourceIp));
                    break;
                case RequestCode.UPDATE_AND_CREATE_TOPIC_LIST:
                    CreateTopicListRequestBody topicListBody = decodeRequiredBody(
                        command, CreateTopicListRequestBody.class, "topic list");
                    if (CollectionUtils.isEmpty(topicListBody.getTopicConfigList())) {
                        throw new AuthorizationException("topic list is empty.");
                    }
                    Set<String> topicListResources = new LinkedHashSet<>();
                    for (TopicConfig topicConfig : topicListBody.getTopicConfigList()) {
                        if (topicConfig == null) {
                            throw new AuthorizationException("topic config is null.");
                        }
                        String topicName = requireResource(topicConfig.getTopicName(), "topic");
                        Resource resource = NamespaceUtil.isRetryTopic(topicName)
                            ? Resource.ofGroup(topicName) : Resource.ofTopic(topicName);
                        addUniqueContext(result, topicListResources, subject, resource, Action.CREATE, sourceIp);
                    }
                    break;
                case RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG:
                    Properties properties = MixAll.string2Properties(
                        decodeRequiredText(command, "cold data flow config"));
                    if (properties == null || properties.isEmpty()) {
                        throw new AuthorizationException("cold data flow config is empty.");
                    }
                    Set<String> coldDataResources = new LinkedHashSet<>();
                    for (String consumerGroup : properties.stringPropertyNames()) {
                        addUniqueContext(result, coldDataResources, subject,
                            Resource.ofGroup(requireResource(consumerGroup, "consumer group")),
                            Action.UPDATE, sourceIp);
                    }
                    break;
                case RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG:
                    group = Resource.ofGroup(requireResource(
                        decodeRequiredText(command, "consumer group"), "consumer group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.UPDATE, sourceIp));
                    break;
                case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                    SubscriptionGroupConfig subscriptionGroupConfig =
                        RemotingSerializable.decode(command.getBody(), SubscriptionGroupConfig.class);
                    if (subscriptionGroupConfig == null
                        || StringUtils.isBlank(subscriptionGroupConfig.getGroupName())) {
                        throw new AuthorizationException("subscription group is null.");
                    }
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofGroup(subscriptionGroupConfig.getGroupName()), Action.CREATE, sourceIp));
                    break;
                case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST:
                    SubscriptionGroupList subscriptionGroupList = decodeRequiredBody(
                        command, SubscriptionGroupList.class, "subscription group list");
                    if (CollectionUtils.isEmpty(subscriptionGroupList.getGroupConfigList())) {
                        throw new AuthorizationException("subscription group list is empty.");
                    }
                    Set<String> subscriptionGroupResources = new LinkedHashSet<>();
                    for (SubscriptionGroupConfig groupConfig : subscriptionGroupList.getGroupConfigList()) {
                        if (groupConfig == null) {
                            throw new AuthorizationException("subscription group config is null.");
                        }
                        addUniqueContext(result, subscriptionGroupResources, subject,
                            Resource.ofGroup(requireResource(groupConfig.getGroupName(), "consumer group")),
                            Action.CREATE, sourceIp);
                    }
                    break;
                case RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC:
                    CreateTopicRequestHeader createTopicRequestHeader =
                        command.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
                    if (createTopicRequestHeader == null) {
                        throw new AuthorizationException("topic header is null.");
                    }
                    String staticTopic = requireResource(createTopicRequestHeader.getTopic(), "topic");
                    TopicQueueMappingDetail mappingDetail = decodeRequiredBody(
                        command, TopicQueueMappingDetail.class, "topic queue mapping");
                    if (!StringUtils.equals(
                        staticTopic, requireResource(mappingDetail.getTopic(), "mapping topic"))) {
                        throw new AuthorizationException("mapping topic does not match topic header.");
                    }
                    topic = Resource.ofTopic(staticTopic);
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.CREATE, sourceIp));
                    break;
                case RequestCode.GET_BROKER_CONFIG:
                case RequestCode.GET_BROKER_RUNTIME_INFO:
                case RequestCode.GET_ALL_CONSUMER_OFFSET:
                case RequestCode.GET_TIMER_CHECK_POINT:
                case RequestCode.GET_ALL_DELAY_OFFSET:
                case RequestCode.GET_BROKER_HA_STATUS:
                case RequestCode.GET_BROKER_EPOCH_CACHE:
                case RequestCode.GET_BROKER_LITE_INFO:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofCluster(authConfig.getClusterName()), Action.GET, sourceIp));
                    break;
                case RequestCode.GET_ALL_TOPIC_CONFIG:
                case RequestCode.GET_TIMER_METRICS:
                case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.of(ResourceType.TOPIC, null, ResourcePattern.ANY), Action.LIST, sourceIp));
                    break;
                case RequestCode.GET_COLD_DATA_FLOW_CTR_INFO:
                case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.of(ResourceType.GROUP, null, ResourcePattern.ANY), Action.LIST, sourceIp));
                    break;
                case RequestCode.GET_ALL_MESSAGE_REQUEST_MODE:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.of(ResourceType.TOPIC, null, ResourcePattern.ANY), Action.LIST, sourceIp));
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.of(ResourceType.GROUP, null, ResourcePattern.ANY), Action.LIST, sourceIp));
                    break;
                case RequestCode.SET_COMMITLOG_READ_MODE:
                case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                case RequestCode.DELETE_EXPIRED_COMMITLOG:
                case RequestCode.CLEAN_UNUSED_TOPIC:
                case RequestCode.POP_ROLLBACK:
                case RequestCode.SWITCH_TIMER_ENGINE:
                    result.add(DefaultAuthorizationContext.of(subject,
                        Resource.ofCluster(authConfig.getClusterName()), Action.UPDATE, sourceIp));
                    break;
                case RequestCode.DELETE_TOPIC_IN_BROKER_LIST:
                    DeleteTopicListRequestBody deleteTopicListRequestBody = decodeRequiredBody(
                        command, DeleteTopicListRequestBody.class, "topic list");
                    if (CollectionUtils.isEmpty(deleteTopicListRequestBody.getTopicList())) {
                        throw new AuthorizationException("topic list is empty.");
                    }
                    Set<String> deleteTopicResources = new LinkedHashSet<>();
                    for (String topicName : deleteTopicListRequestBody.getTopicList()) {
                        String requiredTopic = requireResource(topicName, "topic");
                        Resource resource = NamespaceUtil.isRetryTopic(requiredTopic)
                            ? Resource.ofGroup(requiredTopic) : Resource.ofTopic(requiredTopic);
                        addUniqueContext(result, deleteTopicResources, subject, resource, Action.DELETE, sourceIp);
                    }
                    break;
                case RequestCode.DELETE_SUBSCRIPTION_GROUP_LIST:
                    DeleteSubscriptionGroupListRequestBody deleteGroupListRequestBody = decodeRequiredBody(
                        command, DeleteSubscriptionGroupListRequestBody.class, "subscription group list");
                    if (CollectionUtils.isEmpty(deleteGroupListRequestBody.getGroupNameList())) {
                        throw new AuthorizationException("subscription group list is empty.");
                    }
                    Set<String> deleteGroupResources = new LinkedHashSet<>();
                    for (String groupName : deleteGroupListRequestBody.getGroupNameList()) {
                        group = Resource.ofGroup(requireResource(groupName, "consumer group"));
                        addUniqueContext(result, deleteGroupResources, subject, group, Action.DELETE, sourceIp);
                    }
                    break;
                default:
                    result = buildContextByAnnotation(subject, command, sourceIp);
                    break;
            }
            if (CollectionUtils.isNotEmpty(result)) {
                result.forEach(r -> {
                    r.setChannelId(context.channel().id().asLongText());
                    r.setRpcCode(String.valueOf(command.getCode()));
                });
            }
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Throwable t) {
            throw new AuthorizationException("parse authorization context error.", t);
        }
        return result;
    }

    private static <T> T decodeRequiredBody(RemotingCommand command, Class<T> bodyClass, String bodyName) {
        if (command.getBody() == null || command.getBody().length == 0) {
            throw new AuthorizationException(bodyName + " is null.");
        }
        T body = RemotingSerializable.decode(command.getBody(), bodyClass);
        if (body == null) {
            throw new AuthorizationException(bodyName + " is null.");
        }
        return body;
    }

    private static String decodeRequiredText(RemotingCommand command, String bodyName) {
        if (command.getBody() == null || command.getBody().length == 0) {
            throw new AuthorizationException(bodyName + " is null.");
        }
        return new String(command.getBody(), StandardCharsets.UTF_8);
    }

    private static String requireResource(String resource, String resourceName) {
        if (StringUtils.isBlank(resource)) {
            throw new AuthorizationException(resourceName + " is null.");
        }
        return resource;
    }

    private static void addUniqueContext(List<DefaultAuthorizationContext> contexts,
        Set<String> resources, Subject subject, Resource resource, Action action, String sourceIp) {
        if (resources.add(resource.getResourceKey())) {
            contexts.add(DefaultAuthorizationContext.of(subject, resource, action, sourceIp));
        }
    }

    private List<DefaultAuthorizationContext> buildContextByAnnotation(Subject subject, RemotingCommand request,
        String sourceIp) throws Exception {
        List<DefaultAuthorizationContext> result = new ArrayList<>();

        Class<? extends CommandCustomHeader> clazz = this.requestHeaderRegistry.getRequestHeader(request.getCode());
        if (clazz == null) {
            return result;
        }
        CommandCustomHeader header = request.decodeCommandCustomHeader(clazz);

        RocketMQAction rocketMQAction = clazz.getAnnotation(RocketMQAction.class);
        ResourceType resourceType = rocketMQAction.resource();
        Action[] actions = rocketMQAction.action();
        Resource resource = null;
        if (resourceType == ResourceType.CLUSTER) {
            resource = Resource.ofCluster(authConfig.getClusterName());
        }

        Field[] fields = clazz.getDeclaredFields();
        if (ArrayUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                RocketMQResource rocketMQResource = field.getAnnotation(RocketMQResource.class);
                if (rocketMQResource == null) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    resourceType = rocketMQResource.value();
                    String splitter = rocketMQResource.splitter();
                    Object value = field.get(header);
                    if (value == null) {
                        if (field.getAnnotation(CFNotNull.class) != null) {
                            throw new AuthorizationException(field.getName() + " is null.");
                        }
                        continue;
                    }
                    boolean resourceRequired = field.getAnnotation(CFNotNull.class) != null;
                    String fieldValue = value.toString();
                    if (StringUtils.isBlank(fieldValue)) {
                        if (resourceRequired) {
                            requireResource(fieldValue, field.getName());
                        }
                        continue;
                    }
                    String[] resourceValues;
                    if (StringUtils.isNotBlank(splitter)) {
                        resourceValues = StringUtils.split(fieldValue, splitter);
                    } else {
                        resourceValues = new String[] {fieldValue};
                    }
                    for (String resourceValue : resourceValues) {
                        if (StringUtils.isBlank(resourceValue)) {
                            if (resourceRequired) {
                                requireResource(resourceValue, field.getName());
                            }
                            continue;
                        }
                        if (resourceType == ResourceType.TOPIC && NamespaceUtil.isRetryTopic(resourceValue)) {
                            resource = Resource.ofGroup(resourceValue);
                            result.add(DefaultAuthorizationContext.of(subject, resource, Arrays.asList(actions), sourceIp));
                        } else {
                            resource = Resource.of(resourceType, resourceValue, ResourcePattern.LITERAL);
                            result.add(DefaultAuthorizationContext.of(subject, resource, Arrays.asList(actions), sourceIp));
                        }
                    }
                } finally {
                    field.setAccessible(false);
                }
            }
        }

        if (CollectionUtils.isEmpty(result) && resource != null) {
            result.add(DefaultAuthorizationContext.of(subject, resource, Arrays.asList(actions), sourceIp));
        }

        return result;
    }

    private List<DefaultAuthorizationContext> newContext(Metadata metadata, QueryRouteRequest request) {
        apache.rocketmq.v2.Resource topic = request.getTopic();
        if (StringUtils.isBlank(topic.getName())) {
            throw new AuthorizationException("topic is null.");
        }
        Subject subject = null;
        if (metadata.containsKey(GrpcConstants.AUTHORIZATION_AK)) {
            subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        }
        Resource resource = Resource.ofTopic(topic.getName());
        String sourceIp = StringUtils.substringBeforeLast(metadata.get(GrpcConstants.REMOTE_ADDRESS), CommonConstants.COLON);
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, Arrays.asList(Action.PUB, Action.SUB), sourceIp);
        return Collections.singletonList(context);
    }

    private static List<DefaultAuthorizationContext> newContext(Metadata metadata, TelemetryCommand request) {
        if (request.getCommandCase() != TelemetryCommand.CommandCase.SETTINGS) {
            return null;
        }
        if (!request.getSettings().hasPublishing() && !request.getSettings().hasSubscription()) {
            throw new AclException("settings command doesn't have publishing or subscription.");
        }
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        if (request.getSettings().hasPublishing()) {
            List<apache.rocketmq.v2.Resource> topicList = request.getSettings().getPublishing().getTopicsList();
            for (apache.rocketmq.v2.Resource topic : topicList) {
                result.addAll(newPubContext(metadata, topic));
            }
        }
        if (request.getSettings().hasSubscription()) {
            Subscription subscription = request.getSettings().getSubscription();
            result.addAll(newSubContexts(metadata, ResourceType.GROUP, subscription.getGroup()));
            for (SubscriptionEntry entry : subscription.getSubscriptionsList()) {
                result.addAll(newSubContexts(metadata, ResourceType.TOPIC, entry.getTopic()));
            }
        }
        return result;
    }

    private static List<DefaultAuthorizationContext> newPubContext(Metadata metadata, apache.rocketmq.v2.Resource topic) {
        if (topic == null || StringUtils.isBlank(topic.getName())) {
            throw new AuthorizationException("topic is null.");
        }
        Subject subject = null;
        if (metadata.containsKey(GrpcConstants.AUTHORIZATION_AK)) {
            subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        }
        Resource resource = Resource.ofTopic(topic.getName());
        String sourceIp = StringUtils.substringBeforeLast(metadata.get(GrpcConstants.REMOTE_ADDRESS), CommonConstants.COLON);
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, Action.PUB, sourceIp);
        return Collections.singletonList(context);
    }

    private List<DefaultAuthorizationContext> newSubContexts(Metadata metadata, apache.rocketmq.v2.Resource group,
        apache.rocketmq.v2.Resource topic) {
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        result.addAll(newGroupSubContexts(metadata, group));
        result.addAll(newTopicSubContexts(metadata, topic));
        return result;
    }

    private static List<DefaultAuthorizationContext> newTopicSubContexts(Metadata metadata,
        apache.rocketmq.v2.Resource resource) {
        return newSubContexts(metadata, ResourceType.TOPIC, resource);
    }

    private static List<DefaultAuthorizationContext> newGroupSubContexts(Metadata metadata,
        apache.rocketmq.v2.Resource resource) {
        return newSubContexts(metadata, ResourceType.GROUP, resource);
    }

    private static List<DefaultAuthorizationContext> newSubContexts(Metadata metadata, ResourceType resourceType,
        apache.rocketmq.v2.Resource resource) {
        if (resourceType == ResourceType.GROUP) {
            if (resource == null || StringUtils.isBlank(resource.getName())) {
                throw new AuthorizationException("group is null.");
            }
            return newSubContexts(metadata, Resource.ofGroup(resource.getName()));
        }
        if (resourceType == ResourceType.TOPIC) {
            if (resource == null || StringUtils.isBlank(resource.getName())) {
                throw new AuthorizationException("topic is null.");
            }
            return newSubContexts(metadata, Resource.ofTopic(resource.getName()));
        }
        throw new AuthorizationException("unknown resource type.");
    }

    private static List<DefaultAuthorizationContext> newSubContexts(Metadata metadata, Resource resource) {
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        Subject subject = null;
        if (metadata.containsKey(GrpcConstants.AUTHORIZATION_AK)) {
            subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        }
        String sourceIp = StringUtils.substringBeforeLast(metadata.get(GrpcConstants.REMOTE_ADDRESS), CommonConstants.COLON);
        result.add(DefaultAuthorizationContext.of(subject, resource, Action.SUB, sourceIp));
        return result;
    }
}
