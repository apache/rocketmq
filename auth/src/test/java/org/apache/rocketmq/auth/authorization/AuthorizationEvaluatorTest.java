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

import java.util.ArrayList;
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
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.action.Action;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthorizationEvaluatorTest {

    private AuthConfig authConfig;
    private AuthorizationEvaluator evaluator;
    private AuthenticationMetadataManager authenticationMetadataManager;
    private AuthorizationMetadataManager authorizationMetadataManager;

    @Before
    public void setUp() throws Exception {
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.evaluator = new AuthorizationEvaluator(authConfig);
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(authConfig);
        this.clearAllAcls();
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        this.clearAllAcls();
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void evaluate1() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl).join();

        Subject subject = Subject.of("User:test");
        Resource resource = Resource.ofTopic("test");
        Action action = Action.PUB;
        String sourceIp = "192.168.0.1";
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(subject, resource, action, sourceIp);
        context.setRpcCode("10");
        this.evaluator.evaluate(Collections.singletonList(context));

        // acl sourceIp is null
        acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", null, Decision.GRANT);
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
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*,Group:test*", "Sub", "192.168.0.0/24", Decision.GRANT);
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
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub", "192.168.0.0/24", Decision.GRANT);
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
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl = AuthTestHelper.buildAcl("User:test", "*", "Pub,Sub", "192.168.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl).join();

        acl = AuthTestHelper.buildAcl("User:test", "Topic:*", "Pub,Sub", "192.168.0.0/24", Decision.DENY);
        this.authorizationMetadataManager.updateAcl(acl).join();

        acl = AuthTestHelper.buildAcl("User:test", "Topic:test*", "Pub,Sub", "192.168.0.0/24", Decision.GRANT);
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

        acl = AuthTestHelper.buildAcl("User:test", PolicyType.DEFAULT, "Topic:*", "Pub", null, Decision.GRANT);
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