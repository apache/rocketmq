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
package org.apache.rocketmq.auth.authentication.manager;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.MixAll;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthenticationMetadataManagerTest {

    private AuthConfig authConfig;
    private AuthenticationMetadataManager authenticationMetadataManager;

    @Before
    public void setUp() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
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
    public void createUser() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "test");
        Assert.assertEquals(user.getPassword(), "test");
        Assert.assertEquals(user.getUserType(), UserType.NORMAL);

        user = User.of("super", "super", UserType.SUPER);
        this.authenticationMetadataManager.createUser(user).join();
        user = this.authenticationMetadataManager.getUser("super").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "super");
        Assert.assertEquals(user.getPassword(), "super");
        Assert.assertEquals(user.getUserType(), UserType.SUPER);

        Assert.assertThrows(AuthenticationException.class, () -> {
            try {
                User user2 = User.of("test", "test");
                this.authenticationMetadataManager.createUser(user2).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void updateUser() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "test");
        Assert.assertEquals(user.getPassword(), "test");
        Assert.assertEquals(user.getUserType(), UserType.NORMAL);

        user.setPassword("123");
        this.authenticationMetadataManager.updateUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "test");
        Assert.assertEquals(user.getPassword(), "123");
        Assert.assertEquals(user.getUserType(), UserType.NORMAL);

        user.setUserType(UserType.SUPER);
        this.authenticationMetadataManager.updateUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "test");
        Assert.assertEquals(user.getPassword(), "123");
        Assert.assertEquals(user.getUserType(), UserType.SUPER);

        Assert.assertThrows(AuthenticationException.class, () -> {
            try {
                User user2 = User.of("no_user", "no_user");
                this.authenticationMetadataManager.updateUser(user2).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void deleteUser() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        this.authenticationMetadataManager.deleteUser("test").join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNull(user);

        this.authenticationMetadataManager.deleteUser("no_user").join();
    }

    @Test
    public void getUser() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();
        user = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertNotNull(user);
        Assert.assertEquals(user.getUsername(), "test");
        Assert.assertEquals(user.getPassword(), "test");
        Assert.assertEquals(user.getUserType(), UserType.NORMAL);

        user = this.authenticationMetadataManager.getUser("no_user").join();
        Assert.assertNull(user);
    }

    @Test
    public void listUser() {
        if (MixAll.isMac()) {
            return;
        }
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        Assert.assertTrue(CollectionUtils.isEmpty(users));

        User user = User.of("test-1", "test-1");
        this.authenticationMetadataManager.createUser(user).join();
        users = this.authenticationMetadataManager.listUser(null).join();
        Assert.assertEquals(users.size(), 1);

        user = User.of("test-2", "test-2");
        this.authenticationMetadataManager.createUser(user).join();
        users = this.authenticationMetadataManager.listUser("test").join();
        Assert.assertEquals(users.size(), 2);
    }

    @Test
    public void updateUserDoesNotMutateCachedInstance() {
        if (MixAll.isMac()) {
            return;
        }
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        AuthenticationMetadataProvider provider = AuthenticationFactory.getMetadataProvider(this.authConfig);
        User cachedBefore = provider.getUser("test").join();
        Assert.assertEquals(UserType.NORMAL, cachedBefore.getUserType());
        Assert.assertEquals("test", cachedBefore.getPassword());

        User update = User.of("test", "123", UserType.SUPER);
        this.authenticationMetadataManager.updateUser(update).join();

        // The cached instance must not have been mutated in place by the admin write.
        Assert.assertEquals(UserType.NORMAL, cachedBefore.getUserType());
        Assert.assertEquals("test", cachedBefore.getPassword());

        // A fresh read must reflect the update.
        User updated = this.authenticationMetadataManager.getUser("test").join();
        Assert.assertEquals("123", updated.getPassword());
        Assert.assertEquals(UserType.SUPER, updated.getUserType());
    }

    private void clearAllUsers() {
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> this.authenticationMetadataManager.deleteUser(user.getUsername()).join());
    }
}