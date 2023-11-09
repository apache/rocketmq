package org.apache.rocketmq.auth.authorization.manager;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.helper.AuthorizationTestHelper;
import org.apache.rocketmq.common.action.Action;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AuthorizationMetadataManagerTest {

    private AuthConfig authConfig;

    private AuthorizationMetadataManager authorizationMetadataManager;

    @Before
    public void setUp() throws Exception {
        this.authConfig = AuthorizationTestHelper.createDefaultConfig();
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(this.authConfig);
    }

    @After
    public void tearDown() throws Exception {
        this.authorizationMetadataManager.shutdown();
    }

    @Test
    public void createAcl() {
        Subject subject = User.of("test");
        List<Resource> resources = new ArrayList<>();
        resources.add(Resource.ofTopic("test"));
        List<Action> actions = new ArrayList<>();
        actions.add(Action.CREATE);
        Environment environment = Environment.of("192.168.0.0/24");
        Decision decision = Decision.GRANT;
        Acl acl = Acl.of(subject, resources, actions, environment, decision);
        this.authorizationMetadataManager.createAcl(acl).join();
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
}