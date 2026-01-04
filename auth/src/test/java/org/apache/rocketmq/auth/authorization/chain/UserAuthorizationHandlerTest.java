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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class UserAuthorizationHandlerTest {

    private AuthConfig authConfig;
    private AuthenticationMetadataManager authenticationMetadataManager;
    private UserAuthorizationHandler handler;
    private HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> nextChain;

    @Before
    public void setUp() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
        this.handler = new UserAuthorizationHandler(this.authConfig, null);
        this.nextChain = mock(HandlerChain.class);
        clearAllUsers();
    }

    @After
    public void tearDown() {
        if (MixAll.isMac()) {
            return;
        }
        clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void testUserNotFoundThrows() {
        if (MixAll.isMac()) {
            return;
        }
        User noSuchUser = User.of("no_such_user", "pwd");
        DefaultAuthorizationContext ctx = buildContext(noSuchUser, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
        Assert.assertEquals("User:no_such_user not found.", authorizationException.getMessage());
    }

    @Test
    public void testUserDisabledThrows() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("disabled", "pwd");
        authenticationMetadataManager.createUser(user).join();
        User saved = authenticationMetadataManager.getUser("disabled").join();
        saved.setUserStatus(UserStatus.DISABLE);
        authenticationMetadataManager.updateUser(saved).join();

        DefaultAuthorizationContext ctx = buildContext(user, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        AuthorizationException authorizationException = Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                handler.handle(ctx, nextChain).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });

        Assert.assertEquals("User:disabled is disabled.", authorizationException.getMessage());
        verify(nextChain, never()).handle(any());
    }

    @Test
    public void testSuperUserBypassNextChain() {
        if (MixAll.isMac()) {
            return;
        }
        User superUser = User.of("super", "pwd", UserType.SUPER);
        authenticationMetadataManager.createUser(superUser).join();

        DefaultAuthorizationContext ctx = buildContext(superUser, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        handler.handle(ctx, nextChain).join();
        // super user should bypass the next chain
        verify(nextChain, never()).handle(any());
    }

    @Test
    public void testNormalUserGoesToNextChain() {
        if (MixAll.isMac()) {
            return;
        }
        User normalUser = User.of("normal", "pwd", UserType.NORMAL);
        authenticationMetadataManager.createUser(normalUser).join();

        DefaultAuthorizationContext ctx = buildContext(normalUser, Resource.ofTopic("t1"), Action.SUB, "127.0.0.1");

        when(nextChain.handle(any())).thenReturn(CompletableFuture.completedFuture(null));
        handler.handle(ctx, nextChain).join();
        // normal user should go to the next chain
        verify(nextChain, times(1)).handle(any());
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
}