package org.apache.rocketmq.auth.authorization.manager;

import java.util.ArrayList;
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
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthTestHelper;
import org.apache.rocketmq.common.action.Action;
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

        Subject subject = User.of("test");
        List<Resource> resources = new ArrayList<>();
        resources.add(Resource.ofTopic("test"));
        List<Action> actions = new ArrayList<>();
        actions.add(Action.PUB);
        actions.add(Action.SUB);
        Environment environment = Environment.of("192.168.0.0/24");
        Decision decision = Decision.GRANT;
        Acl acl = Acl.of(subject, resources, actions, environment, decision);
        this.authorizationMetadataManager.createAcl(acl).join();
        acl = this.authorizationMetadataManager.getAcl(subject).join();
        Assert.assertNotNull(acl);
        Assert.assertEquals(acl.getSubject().toSubjectKey(), "User:test");
        Policy policy = acl.getPolicies().get(0);
        Assert.assertNotNull(policy);
        Assert.assertEquals(policy.getPolicyType(), PolicyType.CUSTOM);
        PolicyEntry policyEntry = policy.getEntries().get(0);
        Resource resource = policyEntry.getResource();
        Assert.assertEquals(resource.toResourceKey(), "Topic:test");
        actions = policyEntry.getActions();
        Assert.assertEquals(actions.size(), 2);
        Assert.assertTrue(actions.contains(Action.PUB));
        Assert.assertTrue(actions.contains(Action.SUB));
        environment = policyEntry.getEnvironment();
        Assert.assertEquals(environment.getSourceIps().size(), 1);
        Assert.assertEquals(environment.getSourceIps().get(0), "192.168.0.0/24");
        Assert.assertEquals(policyEntry.getDecision(), Decision.GRANT);

        Assert.assertThrows(AuthorizationException.class, () -> {
            try {
                Subject subject2 = User.of("test");
                List<Resource> resources2 = new ArrayList<>();
                resources2.add(Resource.ofTopic("test"));
                List<Action> actions2 = new ArrayList<>();
                actions2.add(Action.PUB);
                actions2.add(Action.SUB);
                Environment environment2 = Environment.of("192.168.0.0/24");
                Decision decision2 = Decision.GRANT;
                Acl acl2 = Acl.of(subject2, resources2, actions2, environment2, decision2);
                this.authorizationMetadataManager.createAcl(acl2).join();
            } catch (Exception e) {
                AuthTestHelper.handleException(e);
            }
        });
    }

    @Test
    public void updateAcl() {
    }

    @Test
    public void deleteAcl() {
    }

    @Test
    public void getAcl() {
    }

    @Test
    public void listAcl() {
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