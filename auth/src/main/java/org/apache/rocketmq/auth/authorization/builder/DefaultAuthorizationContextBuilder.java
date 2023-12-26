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
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class DefaultAuthorizationContextBuilder implements AuthorizationContextBuilder {

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
                throw new AuthorizationException("message is null");
            }
            result = newPubContext(metadata, request.getMessages(0).getTopic());
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
                throw new AuthorizationException("messageQueue is null");
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
            result = newGroupSubContexts(metadata, request.getGroup());
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
            result.forEach(context -> context.setRpcCode(message.getDescriptorForType().getFullName()));
        }
        return result;
    }

    @Override
    public List<DefaultAuthorizationContext> build(RemotingCommand request, String remoteAddr) {
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        try {
            HashMap<String, String> fields = request.getExtFields();
            Subject subject = User.of(fields.get(SessionCredentials.ACCESS_KEY));
            String sourceIp = StringUtils.substringBefore(remoteAddr, ":");

            Resource topic;
            Resource group;
            switch (request.getCode()) {
                case RequestCode.GET_ROUTEINFO_BY_TOPIC:
                    topic = Resource.ofTopic(fields.get("topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.PUB, Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.SEND_MESSAGE:
                    if (NamespaceUtil.isRetryTopic(fields.get("topic"))) {
                        group = Resource.ofGroup(fields.get("group"));
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(fields.get("topic"));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                case RequestCode.SEND_BATCH_MESSAGE:
                    if (NamespaceUtil.isRetryTopic(fields.get("b"))) {
                        group = Resource.ofGroup(fields.get("a"));
                        result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    } else {
                        topic = Resource.ofTopic(fields.get("b"));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    }
                    break;
                case RequestCode.END_TRANSACTION:
                    topic = Resource.ofTopic(fields.get("topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Action.PUB, sourceIp));
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    group = Resource.ofGroup(fields.get("group"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.PULL_MESSAGE:
                    if (!NamespaceUtil.isRetryTopic(fields.get("topic"))) {
                        topic = Resource.ofTopic(fields.get("topic"));
                        result.add(DefaultAuthorizationContext.of(subject, topic, Action.SUB, sourceIp));
                    }
                    group = Resource.ofGroup(fields.get("consumerGroup"));
                    result.add(DefaultAuthorizationContext.of(subject, group, Action.SUB, sourceIp));
                    break;
                case RequestCode.QUERY_MESSAGE:
                    topic = Resource.ofTopic(fields.get("topic"));
                    result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
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
                        request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    group = Resource.ofGroup(unregisterClientRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.PUB, Action.SUB), sourceIp));
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader =
                        request.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    group = Resource.ofGroup(getConsumerListByGroupRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.QUERY_CONSUMER_OFFSET:
                    final QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader =
                        request.decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
                    if (!NamespaceUtil.isRetryTopic(queryConsumerOffsetRequestHeader.getTopic())) {
                        topic = Resource.ofTopic(queryConsumerOffsetRequestHeader.getTopic());
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    }
                    group = Resource.ofTopic(queryConsumerOffsetRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.GET), sourceIp));
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        request.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    if (!NamespaceUtil.isRetryTopic(updateConsumerOffsetRequestHeader.getTopic())) {
                        topic = Resource.ofTopic(updateConsumerOffsetRequestHeader.getTopic());
                        result.add(DefaultAuthorizationContext.of(subject, topic, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    }
                    group = Resource.ofTopic(updateConsumerOffsetRequestHeader.getConsumerGroup());
                    result.add(DefaultAuthorizationContext.of(subject, group, Arrays.asList(Action.SUB, Action.UPDATE), sourceIp));
                    break;
                default:
                    result = buildContextByAnnotation(subject, request, remoteAddr);
                    break;
            }
            if (CollectionUtils.isNotEmpty(result)) {
                result.forEach(context -> context.setRpcCode(String.valueOf(request.getCode())));
            }
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Throwable t) {
            throw new AuthorizationException("parse authorization context error", t);
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
            throw new AuthorizationException("topic is null");
        }
        User subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        Resource resource = Resource.ofTopic(topic.getName());
        String sourceIp = metadata.get(GrpcConstants.REMOTE_ADDRESS);
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, Arrays.asList(Action.PUB, Action.SUB), sourceIp);
        return Collections.singletonList(context);
    }

    private static List<DefaultAuthorizationContext> newContext(Metadata metadata, TelemetryCommand request) {
        if (request.getCommandCase() != TelemetryCommand.CommandCase.SETTINGS) {
            return null;
        }
        if (!request.getSettings().hasPublishing() && !request.getSettings().hasSubscription()) {
            throw new AclException("settings command doesn't have publishing or subscription");
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
            throw new AuthorizationException("topic is null");
        }
        User subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        Resource resource = Resource.ofTopic(topic.getName());
        String sourceIp = metadata.get(GrpcConstants.REMOTE_ADDRESS);
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
                throw new AuthorizationException("group is null");
            }
            return newSubContexts(metadata, Resource.ofGroup(resource.getName()));
        }
        if (resourceType == ResourceType.TOPIC) {
            if (resource == null || StringUtils.isBlank(resource.getName())) {
                throw new AuthorizationException("topic is null");
            }
            return newSubContexts(metadata, Resource.ofTopic(resource.getName()));
        }
        throw new AuthorizationException("unknown resource type");
    }

    private static List<DefaultAuthorizationContext> newSubContexts(Metadata metadata, Resource resource) {
        List<DefaultAuthorizationContext> result = new ArrayList<>();
        User subject = User.of(metadata.get(GrpcConstants.AUTHORIZATION_AK));
        String sourceIp = metadata.get(GrpcConstants.REMOTE_ADDRESS);
        result.add(DefaultAuthorizationContext.of(subject, resource, Action.SUB, sourceIp));
        return result;
    }
}
