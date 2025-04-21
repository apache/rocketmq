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
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class DefaultAuthorizationContextBuilder implements AuthorizationContextBuilder {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final String A = "a";
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
            if (!isConsumerClientType(request.getClientType())) {
                return null;
            }
            result = newGroupSubContexts(metadata, request.getGroup());
        }
        if (message instanceof ReceiveMessageRequest) {
            ReceiveMessageRequest request = (ReceiveMessageRequest) message;
            if (!request.hasMessageQueue()) {
                throw new AuthorizationException("messageQueue is null.");
            }
            result = newSubContexts(metadata, request.getGroup(), request.getMessageQueue().getTopic());
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
            result = newGroupSubContexts(metadata, request.getGroup());
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
            if (MapUtils.isEmpty(fields)) {
                return result;
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
                    if (NamespaceUtil.isRetryTopic(fields.get(TOPIC))) {
                        group = Resource.ofGroup(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    } else {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.PUB, Action.SUB, Action.GET), sourceIp));
                    }
                    break;
                case RequestCode.SEND_MESSAGE:
                    if (NamespaceUtil.isRetryTopic(fields.get(TOPIC))) {
                        if (StringUtils.isNotBlank(fields.get(GROUP))) {
                            group = Resource.ofGroup(fields.get(GROUP));
                        } else {
                            group = Resource.ofGroup(fields.get(TOPIC));
                        }
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                case RequestCode.SEND_BATCH_MESSAGE:
                    if (NamespaceUtil.isRetryTopic(fields.get(B))) {
                        if (StringUtils.isNotBlank(fields.get(A))) {
                            group = Resource.ofGroup(fields.get(A));
                        } else {
                            group = Resource.ofGroup(fields.get(B));
                        }
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(fields.get(B));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.RECALL_MESSAGE:
                    topic = Resource.ofTopic(fields.get(TOPIC));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    break;
                case RequestCode.END_TRANSACTION:
                    if (StringUtils.isNotBlank(fields.get(TOPIC))) {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    group = Resource.ofGroup(fields.get(GROUP));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.PULL_MESSAGE:
                    if (!NamespaceUtil.isRetryTopic(fields.get(TOPIC))) {
                        topic = Resource.ofTopic(fields.get(TOPIC));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                    }
                    group = Resource.ofGroup(fields.get(CONSUMER_GROUP));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.QUERY_MESSAGE:
                    topic = Resource.ofTopic(fields.get(TOPIC));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(command.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        group = Resource.ofGroup(data.getGroupName());
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            if (NamespaceUtil.isRetryTopic(subscriptionData.getTopic())) {
                                continue;
                            }
                            topic = Resource.ofTopic(subscriptionData.getTopic());
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
                    group = Resource.ofGroup(getConsumerListByGroupRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.QUERY_CONSUMER_OFFSET:
                    final QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader =
                        command.decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
                    if (!NamespaceUtil.isRetryTopic(queryConsumerOffsetRequestHeader.getTopic())) {
                        topic = Resource.ofTopic(queryConsumerOffsetRequestHeader.getTopic());
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    }
                    group = Resource.ofGroup(queryConsumerOffsetRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        command.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    if (!NamespaceUtil.isRetryTopic(updateConsumerOffsetRequestHeader.getTopic())) {
                        topic = Resource.ofTopic(updateConsumerOffsetRequestHeader.getTopic());
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    }
                    group = Resource.ofGroup(updateConsumerOffsetRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    break;
                case RequestCode.LOCK_BATCH_MQ:
                    LockBatchRequestBody lockBatchRequestBody = LockBatchRequestBody.decode(command.getBody(), LockBatchRequestBody.class);
                    group = Resource.ofGroup(lockBatchRequestBody.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    if (CollectionUtils.isNotEmpty(lockBatchRequestBody.getMqSet())) {
                        for (MessageQueue messageQueue : lockBatchRequestBody.getMqSet()) {
                            if (NamespaceUtil.isRetryTopic(messageQueue.getTopic())) {
                                continue;
                            }
                            topic = Resource.ofTopic(messageQueue.getTopic());
                            result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                        }
                    }
                    break;
                case RequestCode.UNLOCK_BATCH_MQ:
                    UnlockBatchRequestBody unlockBatchRequestBody = LockBatchRequestBody.decode(command.getBody(), UnlockBatchRequestBody.class);
                    group = Resource.ofGroup(unlockBatchRequestBody.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    if (CollectionUtils.isNotEmpty(unlockBatchRequestBody.getMqSet())) {
                        for (MessageQueue messageQueue : unlockBatchRequestBody.getMqSet()) {
                            if (NamespaceUtil.isRetryTopic(messageQueue.getTopic())) {
                                continue;
                            }
                            topic = Resource.ofTopic(messageQueue.getTopic());
                            result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                        }
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
                        continue;
                    }
                    String[] resourceValues;
                    if (StringUtils.isNotBlank(splitter)) {
                        resourceValues = StringUtils.split(value.toString(), splitter);
                    } else {
                        resourceValues = new String[] {value.toString()};
                    }
                    for (String resourceValue : resourceValues) {
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

    private boolean isConsumerClientType(ClientType clientType) {
        return Arrays.asList(ClientType.PUSH_CONSUMER, ClientType.SIMPLE_CONSUMER, ClientType.PULL_CONSUMER)
            .contains(clientType);
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
