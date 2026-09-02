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
package org.apache.rocketmq.auth.authorization;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.authorization.strategy.AuthorizationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AuthorizationEvaluatorTest {

    private AuthConfig authConfig;
    private AuthorizationEvaluator evaluator;
    private AuthenticationMetadataManager authenticationMetadataManager;
    private AuthorizationMetadataManager authorizationMetadataManager;

    @Before
    public void setUp() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.evaluator = new AuthorizationEvaluator(authConfig);
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(authConfig);
        this.clearAllAcls();
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.clearAllAcls();
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void evaluate1() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl).join();

        Subject subject = Subject.of("User:test");
        Resource resource = Resource.ofTopic("test");
        Action action = Action.PUB;
        String sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context.setRpcCode("10");
        this.evaluator.evaluate(Collections.singletonList(context));

        // acl sourceIp is null
        acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", null, Decision.ALLOW);
        this.authorizationMetadataManager.updateAcl(acl).join();

        subject = Subject.of("User:test");
        resource = Resource.ofTopic("test");
        action = Action.PUB;
        sourceIp = "192.168.0.1";
        context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context.setRpcCode("10");
        this.evaluator.evaluate(Collections.singletonList(context));
    }

    @Test
    public void evaluate2() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*,Group:test*", "Sub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl).join();

        List<AuthorizationContext> contexts = new ArrayList<>();

        Subject subject = Subject.of("User:test");
        Resource resource = Resource.ofTopic("test");
        Action action = Action.SUB;
        String sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context1 = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context1.setRpcCode("11");
        contexts.add(context1);

        subject = Subject.of("User:test");
        resource = Resource.ofGroup("test");
        action = Action.SUB;
        sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context2 = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context2.setRpcCode("11");
        contexts.add(context2);

        this.evaluator.evaluate(contexts);
    }

    @Test
    public void evaluate4() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl).join();

        // user not exist
        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:abc");
            Resource resource = Resource.ofTopic("test");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        // resource not match
        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("abc");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        // action not match
        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test");
            Action action = Action.SUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        // sourceIp not match
        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test");
            Action action = Action.PUB;
            String sourceIp = "10.10.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        // decision is deny
        acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.updateAcl(acl).join();
        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });
    }

    @Test
    public void evaluate5() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "*", "Pub,Sub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl).join();

        acl = AuthTestHelper.buildAcl("User:test", "Topic:*", "Pub,Sub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.updateAcl(acl).join();

        acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub,Sub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.updateAcl(acl).join();

        acl = AuthTestHelper.buildAcl("User:test", "Topic:test-1", "Pub,Sub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.updateAcl(acl).join();

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test-1");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test-2");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        }

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("abc");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofGroup("test-2");
            Action action = Action.SUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        }
    }

    @Test
    public void evaluate6() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig.setAuthorizationWhitelist("10");
        this.evaluator = new AuthorizationEvaluator(this.authConfig);

        Subject subject = Subject.of("User:test");
        Resource resource = Resource.ofTopic("test");
        Action action = Action.PUB;
        String sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context.setRpcCode("10");
        this.evaluator.evaluate(Collections.singletonList(context));
    }

    @Test
    public void evaluate7() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig.setAuthorizationEnabled(false);
        this.evaluator = new AuthorizationEvaluator(this.authConfig);

        Subject subject = Subject.of("User:test");
        Resource resource = Resource.ofTopic("test");
        Action action = Action.PUB;
        String sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context.setRpcCode("10");
        this.evaluator.evaluate(Collections.singletonList(context));
    }

    @Test
    public void evaluate8() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.createAcl(acl).join();

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("abc");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        acl = AuthTestHelper.buildAcl("User:test", PolicyType.DEFAULT, "Topic:*", "Pub", null, Decision.ALLOW);
        this.authorizationMetadataManager.updateAcl(acl).join();
        {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("abc");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        }
    }

    @Test
    public void evaluate9() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl0 = AuthTestHelper.buildAcl("User:test", "*", "Pub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl0).join();
        Acl acl1 = AuthTestHelper.buildAcl("User:test", "Topic:*", "Pub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl1).join();
        Acl acl2 = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.ALLOW);
        this.authorizationMetadataManager.createAcl(acl2).join();
        Acl acl3 = AuthTestHelper.buildAcl("User:test", "Topic:test_*", "Pub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.createAcl(acl3).join();
        Acl acl4 = AuthTestHelper.buildAcl("User:test", "Topic:test_001", "Pub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.createAcl(acl4).join();

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test_001");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });

        Assert.assertThrows(AuthorizationException.class, () -> {
            Subject subject = Subject.of("User:test");
            Resource resource = Resource.ofTopic("test_002");
            Action action = Action.PUB;
            String sourceIp = "192.168.0.1";
            DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
            context.setRpcCode("10");
            this.evaluator.evaluate(Collections.singletonList(context));
        });
    }

    @Test
    public void evaluateTypedAnyListResources() {
        Assume.assumeFalse(MixAll.isMac());
        User listUser = User.of("list-user", "test");
        User getUser = User.of("get-user", "test");
        User literalUser = User.of("literal-user", "test");
        User topicUser = User.of("topic-user", "test");
        this.authenticationMetadataManager.createUser(listUser).join();
        this.authenticationMetadataManager.createUser(getUser).join();
        this.authenticationMetadataManager.createUser(literalUser).join();
        this.authenticationMetadataManager.createUser(topicUser).join();

        this.authorizationMetadataManager.createAcl(AuthTestHelper.buildAcl(
            "User:list-user", "Topic:*,Group:*", "List", null, Decision.ALLOW)).join();
        this.authorizationMetadataManager.createAcl(AuthTestHelper.buildAcl(
            "User:get-user", "Topic:*,Group:*", "Get", null, Decision.ALLOW)).join();
        this.authorizationMetadataManager.createAcl(AuthTestHelper.buildAcl(
            "User:literal-user", "Topic:orders,Group:consumers", "List", null, Decision.ALLOW)).join();
        this.authorizationMetadataManager.createAcl(AuthTestHelper.buildAcl(
            "User:topic-user", "Topic:*", "List", null, Decision.ALLOW)).join();

        this.evaluator.evaluate(Arrays.asList(
            typedAnyListContext("list-user", ResourceType.TOPIC),
            typedAnyListContext("list-user", ResourceType.GROUP)));

        Assert.assertThrows(AuthorizationException.class, () -> this.evaluator.evaluate(
            Collections.singletonList(typedAnyListContext("get-user", ResourceType.TOPIC))));
        Assert.assertThrows(AuthorizationException.class, () -> this.evaluator.evaluate(
            Collections.singletonList(typedAnyListContext("literal-user", ResourceType.TOPIC))));

        this.evaluator.evaluate(
            Collections.singletonList(typedAnyListContext("topic-user", ResourceType.TOPIC)));
        Assert.assertThrows(AuthorizationException.class, () -> this.evaluator.evaluate(
            Collections.singletonList(typedAnyListContext("topic-user", ResourceType.GROUP))));
    }

    @Test
    public void rejectsEmptyContextWithoutRequest() {
        AuthorizationEvaluator requestEvaluator = new AuthorizationEvaluator(mock(AuthorizationStrategy.class));

        Assert.assertThrows(AuthorizationException.class, () -> requestEvaluator.evaluate(null));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(Collections.emptyList()));
    }

    @Test
    public void evaluatesNonEmptyContextBeforeCompatibilityMatching() {
        AuthorizationStrategy strategy = mock(AuthorizationStrategy.class);
        AuthorizationEvaluator requestEvaluator = new AuthorizationEvaluator(strategy);
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(
            Subject.of("User:test"), Resource.ofTopic("topic"), Action.PUB, "127.0.0.1");

        requestEvaluator.evaluate(RemotingCommand.createRequestCommand(-1, null),
            Collections.singletonList(context));

        verify(strategy).evaluate(context);
    }

    @Test
    public void acceptsOnlyResourceLessRemotingCompatibilityShapes() {
        AuthorizationEvaluator requestEvaluator = new AuthorizationEvaluator(mock(AuthorizationStrategy.class));

        requestEvaluator.evaluate(producerHeartbeat("producerGroup"), Collections.emptyList());
        requestEvaluator.evaluate(producerHeartbeat(" "), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(producerHeartbeat(null), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(remotingRequest(
                RequestCode.HEART_BEAT, new HeartbeatData().encode()), Collections.emptyList()));
        HeartbeatData invalidHeartbeat = new HeartbeatData();
        invalidHeartbeat.setProducerDataSet(Collections.singleton(null));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(remotingRequest(
                RequestCode.HEART_BEAT, invalidHeartbeat.encode()), Collections.emptyList()));

        HeartbeatData mixedHeartbeat = new HeartbeatData();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("producerGroup");
        mixedHeartbeat.setProducerDataSet(Collections.singleton(producerData));
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("consumerGroup");
        mixedHeartbeat.setConsumerDataSet(Collections.singleton(consumerData));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(remotingRequest(RequestCode.HEART_BEAT, mixedHeartbeat.encode()),
                Collections.emptyList()));

        requestEvaluator.evaluate(unregister("producerGroup", null), Collections.emptyList());
        requestEvaluator.evaluate(unregister(null, "producerGroup", null), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(unregister(null, null), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(unregister("producerGroup", "consumerGroup"),
                Collections.emptyList()));

        requestEvaluator.evaluate(endTransaction(true), Collections.emptyList());
        requestEvaluator.evaluate(endTransaction(false), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(
                RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, null),
                Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(endTransaction(
                "topic", 1L, 2L, MessageSysFlag.TRANSACTION_COMMIT_TYPE, "messageId"),
                Collections.emptyList()));
        requestEvaluator.evaluate(endTransaction(
            null, -1L, 2L, MessageSysFlag.TRANSACTION_COMMIT_TYPE, "messageId"), Collections.emptyList());
        requestEvaluator.evaluate(endTransaction(
            null, 1L, -1L, MessageSysFlag.TRANSACTION_COMMIT_TYPE, "messageId"), Collections.emptyList());
        requestEvaluator.evaluate(endTransaction(
            null, 1L, 2L, 99, "messageId"), Collections.emptyList());
        requestEvaluator.evaluate(endTransaction(
            null, 1L, 2L, null, "messageId"), Collections.emptyList());

        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(viewMessage(0L), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(viewMessage(-1L), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(viewMessage(null), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(
                RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, null),
                Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(viewMessage("topic", 0L), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(RemotingCommand.createRequestCommand(-1, null),
                Collections.emptyList()));
    }

    @Test
    public void acceptsOnlyResourceLessGrpcCompatibilityShapes() {
        AuthorizationEvaluator requestEvaluator = new AuthorizationEvaluator(mock(AuthorizationStrategy.class));

        requestEvaluator.evaluate(HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build(), Collections.emptyList());
        requestEvaluator.evaluate(HeartbeatRequest.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build(), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(HeartbeatRequest.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setGroup(apache.rocketmq.v2.Resource.newBuilder().setName("consumerGroup"))
                .build(), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(HeartbeatRequest.newBuilder()
                .setClientType(ClientType.PUSH_CONSUMER)
                .build(), Collections.emptyList()));

        requestEvaluator.evaluate(NotifyClientTerminationRequest.getDefaultInstance(), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(NotifyClientTerminationRequest.newBuilder()
                .setGroup(apache.rocketmq.v2.Resource.newBuilder().setName("consumerGroup"))
                .build(), Collections.emptyList()));

        requestEvaluator.evaluate(TelemetryCommand.newBuilder()
            .setThreadStackTrace(ThreadStackTrace.newBuilder().setNonce("nonce"))
            .build(), Collections.emptyList());
        requestEvaluator.evaluate(TelemetryCommand.newBuilder()
            .setVerifyMessageResult(VerifyMessageResult.newBuilder().setNonce("nonce"))
            .build(), Collections.emptyList());
        requestEvaluator.evaluate(TelemetryCommand.newBuilder()
            .setThreadStackTrace(ThreadStackTrace.getDefaultInstance())
            .build(), Collections.emptyList());
        requestEvaluator.evaluate(TelemetryCommand.newBuilder()
            .setVerifyMessageResult(VerifyMessageResult.getDefaultInstance())
            .build(), Collections.emptyList());
        requestEvaluator.evaluate(TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder().setPublishing(Publishing.getDefaultInstance()))
            .build(), Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(TelemetryCommand.newBuilder()
                .setSettings(Settings.newBuilder().setPublishing(Publishing.newBuilder()
                    .addTopics(apache.rocketmq.v2.Resource.newBuilder().setName("topic"))))
                .build(), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(TelemetryCommand.getDefaultInstance(), Collections.emptyList()));
        Assert.assertThrows(AuthorizationException.class,
            () -> requestEvaluator.evaluate(QueryRouteRequest.getDefaultInstance(), Collections.emptyList()));
    }

    private RemotingCommand producerHeartbeat(String producerGroup) {
        HeartbeatData heartbeatData = new HeartbeatData();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName(producerGroup);
        heartbeatData.setProducerDataSet(Collections.singleton(producerData));
        return remotingRequest(RequestCode.HEART_BEAT, heartbeatData.encode());
    }

    private RemotingCommand unregister(String producerGroup, String consumerGroup) {
        return unregister("clientId", producerGroup, consumerGroup);
    }

    private RemotingCommand unregister(String clientId, String producerGroup, String consumerGroup) {
        UnregisterClientRequestHeader header = new UnregisterClientRequestHeader();
        header.setClientID(clientId);
        header.setProducerGroup(producerGroup);
        header.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand endTransaction(boolean complete) {
        return endTransaction(null, 1L, 2L, MessageSysFlag.TRANSACTION_COMMIT_TYPE,
            complete ? "messageId" : null);
    }

    private RemotingCommand endTransaction(String topic, Long transactionOffset,
        Long commitLogOffset, Integer state, String messageId) {
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTopic(topic);
        header.setProducerGroup("producerGroup");
        header.setTranStateTableOffset(transactionOffset);
        header.setCommitLogOffset(commitLogOffset);
        header.setCommitOrRollback(state);
        header.setMsgId(messageId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand viewMessage(Long offset) {
        return viewMessage(null, offset);
    }

    private RemotingCommand viewMessage(String topic, Long offset) {
        ViewMessageRequestHeader header = new ViewMessageRequestHeader();
        header.setTopic(topic);
        header.setOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand remotingRequest(int requestCode, byte[] body) {
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
        request.setBody(body);
        return request;
    }

    private DefaultAuthorizationContext typedAnyListContext(String username, ResourceType resourceType) {
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(
            Subject.of("User:" + username),
            Resource.of(resourceType, null, ResourcePattern.ANY),
            Action.LIST,
            "192.168.0.1");
        context.setRpcCode(String.valueOf(RequestCode.GET_ALL_TOPIC_CONFIG));
        return context;
    }

    private void clearAllUsers() {
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> this.authenticationMetadataManager.deleteUser(user.getUsername()).join());
    }

    private void clearAllAcls() {
        List<Acl> acls = this.authorizationMetadataManager.listAcl(null, null).join();
        if (CollectionUtils.isEmpty(acls)) {
            return;
        }
        acls.forEach(acl -> this.authorizationMetadataManager.deleteAcl(acl.getSubject(), null, null).join());
    }
}
