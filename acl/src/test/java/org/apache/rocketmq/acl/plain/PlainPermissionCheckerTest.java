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

package org.apache.rocketmq.acl.plain;

import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class PlainPermissionCheckerTest {

    private PlainPermissionChecker permissionChecker;

    @Before
    public void setUp() {
        permissionChecker = new PlainPermissionChecker();
    }

    @Test
    public void testCheck_withAdminPermission_shouldPass() {
        PlainAccessResource checkedAccess = new PlainAccessResource();
        checkedAccess.setRequestCode(Permission.SUB);
        checkedAccess.addResourceAndPerm("topic1", Permission.PUB);

        PlainAccessResource ownedAccess = new PlainAccessResource();
        ownedAccess.setAccessKey("adminUser");
        ownedAccess.setAdmin(true);

        try {
            permissionChecker.check(checkedAccess, ownedAccess);
        } catch (AclException e) {
            Assert.fail("Should not throw any exception for admin user");
        }
    }

    @Test(expected = AclException.class)
    public void testCheck_withoutAdminPermissionAndNoDefaultPerm_shouldThrowAclException() {
        PlainAccessResource checkedAccess = new PlainAccessResource();
        checkedAccess.setRequestCode(Permission.SUB);
        checkedAccess.addResourceAndPerm("topic1", Permission.PUB);

        PlainAccessResource ownedAccess = new PlainAccessResource();
        ownedAccess.setAccessKey("nonAdminUser");
        ownedAccess.setAdmin(false);

        permissionChecker.check(checkedAccess, ownedAccess);
    }

    @Test
    public void testCheck_withDefaultPermissions_shouldPass() {
        PlainAccessResource checkedAccess = new PlainAccessResource();
        checkedAccess.setRequestCode(Permission.SUB);
        checkedAccess.addResourceAndPerm("topic1", Permission.PUB);

        PlainAccessResource ownedAccess = new PlainAccessResource();
        ownedAccess.setAccessKey("nonAdminUser");
        ownedAccess.setAdmin(false);
        ownedAccess.setDefaultTopicPerm(Permission.PUB);

        try {
            permissionChecker.check(checkedAccess, ownedAccess);
        } catch (AclException e) {
            Assert.fail("Should not throw any exception for default permissions");
        }
    }

    @Test(expected = AclException.class)
    public void testCheck_withoutPermission_shouldThrowAclException() {
        PlainAccessResource checkedAccess = new PlainAccessResource();
        checkedAccess.setRequestCode(Permission.SUB);
        checkedAccess.addResourceAndPerm("topic1", Permission.PUB);

        PlainAccessResource ownedAccess = new PlainAccessResource();
        ownedAccess.setAccessKey("nonAdminUser");
        ownedAccess.setAdmin(false);
        ownedAccess.setDefaultTopicPerm(Permission.SUB); // Default permission is not enough for the needed permission

        permissionChecker.check(checkedAccess, ownedAccess);
    }

}
