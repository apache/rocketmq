package org.apache.rocketmq.acl.plain;

import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

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
