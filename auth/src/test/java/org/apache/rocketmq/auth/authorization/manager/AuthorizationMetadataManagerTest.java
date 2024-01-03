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
package org.apache.rocketmq.auth.authorization.manager;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthorizationMetadataManagerTest {

    private AuthConfig authConfig;

    private AuthenticationMetadataManager authenticationMetadataManager;

    private AuthorizationMetadataManager authorizationMetadataManager;

    @Before
    public void setUp() throws Exception {
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(this.authConfig);
        this.clearAllAcls();
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        this.clearAllAcls();
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
        this.authorizationMetadataManager.shutdown();
    }

    @Test
    public void createAcl() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl1 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl1).join();
        Acl acl2 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl1, acl2));

        user = User.of("abc", "abc");
        this.authenticationMetadataManager.createUser(user).join();

        acl1 = AuthTestHelper.buildAcl("User:abc", PolicyType.DEFAULT, "Topic:*,Group:*", "PUB,SUB",
            null, Decision.DENY);
        this.authorizationMetadataManager.createAcl(acl1).join();
        acl2 = this.authorizationMetadataManager.getAcl(Subject.of("User:abc")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl1, acl2));

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                Acl acl3 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test", "PUB,SUB",
                    "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
                this.authorizationMetadataManager.createAcl(acl3).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                Acl acl3 = AuthTestHelper.buildAcl("User:ddd", "Topic:test,Group:test", "PUB,SUB",
                    "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
                this.authorizationMetadataManager.createAcl(acl3).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void updateAcl() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl1 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl1).join();

        Acl acl2 = AuthTestHelper.buildAcl("User:test", "Topic:abc,Group:abc", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.updateAcl(acl2).join();

        Acl acl3 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test,Topic:abc,Group:abc", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        Acl acl4 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl3, acl4));

        Policy policy = AuthTestHelper.buildPolicy("Topic:test,Group:test", "PUB,SUB,Create", "192.168.0.0/24", Decision.DENY);
        acl4.updatePolicy(policy);
        this.authorizationMetadataManager.updateAcl(acl4);
        Acl acl5 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl4, acl5));

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                User user2 = User.of("abc", "abc");
                this.authenticationMetadataManager.createUser(user2).join();
                Acl acl6 = AuthTestHelper.buildAcl("User:abc", "Topic:test,Group:test", "PUB,SUB",
                    "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
                this.authorizationMetadataManager.updateAcl(acl6).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void deleteAcl() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl1 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl1).join();

        this.authorizationMetadataManager.deleteAcl(Subject.of("User:test"), PolicyType.CUSTOM, Resource.ofTopic("abc")).join();
        Acl acl2 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl1, acl2));

        this.authorizationMetadataManager.deleteAcl(Subject.of("User:test"), PolicyType.CUSTOM, Resource.ofTopic("test")).join();
        Acl acl3 = AuthTestHelper.buildAcl("User:test", "Group:test", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        Acl acl4 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl3, acl4));

        this.authorizationMetadataManager.deleteAcl(Subject.of("User:test"));
        Acl acl5 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertNull(acl5);

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                this.authorizationMetadataManager.deleteAcl(Subject.of("User:abc")).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void getAcl() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user).join();

        Acl acl1 = AuthTestHelper.buildAcl("User:test", "Topic:test,Group:test", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl1).join();
        Acl acl2 = this.authorizationMetadataManager.getAcl(Subject.of("User:test")).join();
        Assert.assertTrue(AuthTestHelper.isEquals(acl1, acl2));

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                this.authorizationMetadataManager.getAcl(Subject.of("User:abc")).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void listAcl() {
        User user1 = User.of("test-1", "test-1");
        this.authenticationMetadataManager.createUser(user1).join();
        User user2 = User.of("test-2", "test-2");
        this.authenticationMetadataManager.createUser(user2).join();

        Acl acl1 = AuthTestHelper.buildAcl("User:test-1", "Topic:test-1,Group:test-1", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl1).join();

        Acl acl2 = AuthTestHelper.buildAcl("User:test-2", "Topic:test-2,Group:test-2", "PUB,SUB",
            "192.168.0.0/24,10.10.0.0/24", Decision.GRANT);
        this.authorizationMetadataManager.createAcl(acl2).join();

        List<Acl> acls1 = this.authorizationMetadataManager.listAcl(null, null).join();
        Assert.assertEquals(acls1.size(), 2);

        List<Acl> acls2 = this.authorizationMetadataManager.listAcl("User:test-1", null).join();
        Assert.assertEquals(acls2.size(), 1);

        List<Acl> acls3 = this.authorizationMetadataManager.listAcl("test", null).join();
        Assert.assertEquals(acls3.size(), 2);


        List<Acl> acls4 = this.authorizationMetadataManager.listAcl(null, "Topic:test-1").join();
        Assert.assertEquals(acls4.size(), 1);
        Assert.assertEquals(acls4.get(0).getPolicy(PolicyType.CUSTOM).getEntries().size(), 1);

        List<Acl> acls5 = this.authorizationMetadataManager.listAcl(null, "test-1").join();
        Assert.assertEquals(acls5.size(), 1);
        Assert.assertEquals(acls4.get(0).getPolicy(PolicyType.CUSTOM).getEntries().size(), 1);

        List<Acl> acls6 = this.authorizationMetadataManager.listAcl("User:abc", null).join();
        Assert.assertTrue(CollectionUtils.isEmpty(acls6));

        List<Acl> acls7 = this.authorizationMetadataManager.listAcl(null, "Topic:abc").join();
        Assert.assertTrue(CollectionUtils.isEmpty(acls7));
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