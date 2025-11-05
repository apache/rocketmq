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
package org.apache.rocketmq.auth.authorization.chain;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

public class AclAuthorizationHandlerTest {

    private AuthConfig authConfig;
    private AuthenticationMetadataManager authenticationMetadataManager;
    private AuthorizationMetadataManager authorizationMetadataManager;
    private AclAuthorizationHandler handler;
    private HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> nextChain;

    @Before
    public void setUp() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(this.authConfig);
        this.handler = new AclAuthorizationHandler(this.authConfig);
        this.nextChain = mock(HandlerChain.class);
        clearAllAcls();
        clearAllUsers();
    }

    @After
    public void tearDown() {
        if (MixAll.isMac()) {
            return;
        }
        clearAllAcls();
        clearAllUsers();
        this.authenticationMetadataManager.shutdown();
        this.authorizationMetadataManager.shutdown();
    }

    @Test
    public void testNoAclThrows() {
        if (MixAll.isMac()) {
            return;
        }
        // Create a user with no ACL entries.
        User user = User.of("noacl", "pwd");
        authenticationMetadataManager.createUser(user).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:noacl has no permission to access Topic:t1 from 127.0.0.1, no matched policies.",
                authorizationException.getMessage());
    }

    @Test
    public void testNoMatchedPolicyThrows() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("no_match_acl", "pwd");
        authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:no_match_acl", "Topic:abc", Action.SUB.getName(), null, Decision.ALLOW);
        authorizationMetadataManager.createAcl(acl).join();

        // Ensure an ACL has been created.
        List<Acl> acls = authorizationMetadataManager.listAcl(null, null).join();
        Assert.assertEquals(1, acls.size());

        // The requested resource does not match any ACL entry.
        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:no_match_acl has no permission to access Topic:t1 from 127.0.0.1, no matched policies.",
                authorizationException.getMessage());
    }

    @Test
    public void testDecisionDenyThrows() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("deny", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The ACL entry matches, but the decision is DENY.
        Acl acl = AuthTestHelper.buildAcl("User:deny", "Topic:t1", Action.SUB.getName(), null, Decision.DENY);
        authorizationMetadataManager.createAcl(acl).join();

        List<Acl> acls = authorizationMetadataManager.listAcl(null, null).join();
        Assert.assertEquals(1, acls.size());

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:deny has no permission to access Topic:t1 from 127.0.0.1, the decision is deny.",
                authorizationException.getMessage());
    }

    @Test
    public void testAllowDoesNotThrow() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("allow", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The ACL matches and the decision is ALLOW.
        Acl acl = AuthTestHelper.buildAcl("User:allow", "Topic:t1", Action.SUB.getName(), null, Decision.ALLOW);
        authorizationMetadataManager.createAcl(acl).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        handler.handle(ctx, nextChain).join();
    }

    @Test
    public void testDenyBeatsAllow() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // Set up policy entries with both ALLOW and DENY for the same resource.
        Resource resource = Resource.of(ResourceType.TOPIC, "t1", ResourcePattern.LITERAL);
        PolicyEntry allowLiteral = PolicyEntry.of(resource, Collections.singletonList(Action.SUB), null, Decision.ALLOW);
        PolicyEntry denyLiteral = PolicyEntry.of(resource, Collections.singletonList(Action.SUB), null, Decision.DENY);

        // Include both entries in the policy to verify precedence.
        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(allowLiteral, denyLiteral, allowLiteral)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, resource, Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Throwable e) {
                AuthTestHelper.handleException(e);
            }
        });
        // DENY should take precedence.
        Assert.assertEquals("User:user has no permission to access Topic:t1 from 127.0.0.1, the decision is deny.", authorizationException.getMessage());
    }

    @Test
    public void testPrefixedLongerDenyBeatsPrefixedShorterAllow() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The longer PREFIXED DENY policy entry should take precedence over the shorter PREFIXED ALLOW policy entry.
        PolicyEntry denyLonger = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1-abc", ResourcePattern.PREFIXED),
                Collections.singletonList(Action.SUB), null, Decision.DENY);
        PolicyEntry allowShorter = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1-", ResourcePattern.PREFIXED),
                Collections.singletonList(Action.SUB), null, Decision.ALLOW);

        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(allowShorter, denyLonger)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1-abcd"), Action.SUB, "127.0.0.1");
        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Throwable e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:user has no permission to access Topic:t1-abcd from 127.0.0.1, the decision is deny.", authorizationException.getMessage());
    }

    @Test
    public void testLiteralAllowBeatsPrefixedDeny() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The LITERAL ALLOW policy entry should take precedence over the PREFIXED DENY policy entry.
        PolicyEntry allowLiteral = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1", ResourcePattern.LITERAL),
                Collections.singletonList(Action.SUB), null, Decision.ALLOW);
        PolicyEntry denyPrefixed = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t", ResourcePattern.PREFIXED),
                Collections.singletonList(Action.SUB), null, Decision.DENY);

        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(denyPrefixed, allowLiteral)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");
        handler.handle(ctx, nextChain).join();
    }

    @Test
    public void testTopicTypeAllowBeatsAnyTypeDeny() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The ALLOW policy entry with resource type TOPIC should take precedence over the DENY policy entry with resource type ANY.
        PolicyEntry denyAnyType = PolicyEntry.of(
                Resource.of(ResourceType.ANY, "t1", ResourcePattern.LITERAL),
                Collections.singletonList(Action.SUB), null, Decision.DENY);
        PolicyEntry allowTopicType = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1", ResourcePattern.LITERAL),
                Collections.singletonList(Action.SUB), null, Decision.ALLOW);

        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(allowTopicType, denyAnyType)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");
        handler.handle(ctx, nextChain).join();
    }

    @Test
    public void testPrefixedPatternAllowBeatsAnyPatternDeny() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The PREFIXED pattern ALLOW policy entry should take precedence over the ANY pattern DENY policy entry.
        PolicyEntry denyAny = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, null, ResourcePattern.ANY),
                Collections.singletonList(Action.SUB), null, Decision.DENY);
        PolicyEntry allowPrefixed = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1", ResourcePattern.PREFIXED),
                Collections.singletonList(Action.SUB), null, Decision.ALLOW);

        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(allowPrefixed, denyAny)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");
        handler.handle(ctx, nextChain).join();
    }

    @Test
    public void testLiteralPatternDenyBeatsAnyPatternAllow() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("user", "pwd");
        authenticationMetadataManager.createUser(user).join();

        // The LITERAL pattern DENY policy entry should take precedence over the ANY pattern ALLOW policy entry.
        PolicyEntry allowAny = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, null, ResourcePattern.ANY),
                Collections.singletonList(Action.SUB), null, Decision.ALLOW);
        PolicyEntry denyLiteral = PolicyEntry.of(
                Resource.of(ResourceType.TOPIC, "t1", ResourcePattern.LITERAL),
                Collections.singletonList(Action.SUB), null, Decision.DENY);


        Policy policy = Policy.of(PolicyType.CUSTOM, new ArrayList<>(Arrays.asList(allowAny, denyLiteral)));
        authorizationMetadataManager.createAcl(Acl.of(user, policy)).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");
        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Throwable e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:user has no permission to access Topic:t1 from 127.0.0.1, the decision is deny.", authorizationException.getMessage());
    }

    private DefaultAuthorizationContext buildContext(Subject subject, Resource resource, Action action, String sourceIp) {
        return DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
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
