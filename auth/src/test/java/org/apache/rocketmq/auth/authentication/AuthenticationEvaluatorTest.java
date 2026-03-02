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
package org.apache.rocketmq.auth.authentication;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.MixAll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthenticationEvaluatorTest {

    private AuthConfig authConfig;
    private AuthenticationEvaluator evaluator;
    private AuthenticationMetadataManager authenticationMetadataManager;

    @Before
    public void setUp() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.evaluator = new AuthenticationEvaluator(authConfig);
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void evaluate1() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user);

        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("DJRRXBXlCVuKh6ULoN87847QX+Y=");
        this.evaluator.evaluate(context);
    }

    @Test
    public void evaluate2() {
        if (MixAll.isMac()) {
            return;
        }
        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("DJRRXBXlCVuKh6ULoN87847QX+Y=");
        Assert.assertThrows(AuthenticationException.class, () -> this.evaluator.evaluate(context));
    }

    @Test
    public void evaluate3() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user);

        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("test");
        Assert.assertThrows(AuthenticationException.class, () -> this.evaluator.evaluate(context));
    }

    @Test
    public void evaluate4() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig.setAuthenticationWhitelist("11");
        this.evaluator = new AuthenticationEvaluator(authConfig);

        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("test");
        this.evaluator.evaluate(context);
    }

    @Test
    public void evaluate5() {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig.setAuthenticationEnabled(false);
        this.evaluator = new AuthenticationEvaluator(authConfig);

        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("test");
        this.evaluator.evaluate(context);
    }

    private void clearAllUsers() {
        if (MixAll.isMac()) {
            return;
        }
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> this.authenticationMetadataManager.deleteUser(user.getUsername()).join());
    }
}