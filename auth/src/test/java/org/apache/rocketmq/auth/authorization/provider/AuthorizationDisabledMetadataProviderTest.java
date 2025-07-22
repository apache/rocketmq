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
package org.apache.rocketmq.auth.authorization.provider;

import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AuthorizationDisabledMetadataProviderTest {

    private AuthorizationDisabledMetadataProvider authorizationDisabledMetadataProvider;

    @Before
    public void setUp() throws Exception {
        this.authorizationDisabledMetadataProvider = AuthorizationDisabledMetadataProvider.INSTANCE;
        this.authorizationDisabledMetadataProvider.initialize(new AuthConfig(), null);
    }

    @After
    public void tearDown() throws Exception {
        this.authorizationDisabledMetadataProvider.shutdown();
    }

    @Test
    public void createAcl() {
        CompletableFuture<Void> future = this.authorizationDisabledMetadataProvider.createAcl(new Acl());
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void deleteAcl() {
        CompletableFuture<Void> future = this.authorizationDisabledMetadataProvider.deleteAcl(User.of("username"));
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void updateAcl() {
        CompletableFuture<Void> future = this.authorizationDisabledMetadataProvider.updateAcl(new Acl());
        Assert.assertTrue(future.isDone());
    }

    @Test
    public void getAcl() {
        CompletableFuture<Acl> future = this.authorizationDisabledMetadataProvider.getAcl(User.of("username"));
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.join());
    }

    @Test
    public void listAcl() {
        CompletableFuture<List<Acl>> future = this.authorizationDisabledMetadataProvider.listAcl(null, null);
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.join());
    }
}