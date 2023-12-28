package org.apache.rocketmq.auth.authentication.manager;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthenticationMetadataManagerTest {

    private AuthConfig authConfig;
    private AuthenticationMetadataManager authenticationMetadataManager;

    @Before
    public void setUp() throws Exception {
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void createUser() {
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

    private void clearAllUsers() {
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> this.authenticationMetadataManager.deleteUser(user.getUsername()).join());
    }
}