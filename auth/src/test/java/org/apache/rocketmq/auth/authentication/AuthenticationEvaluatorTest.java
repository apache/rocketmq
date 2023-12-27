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
        this.authConfig = AuthTestHelper.createDefaultConfig();
        this.evaluator = new AuthenticationEvaluator(authConfig);
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.clearAllUsers();
    }

    @After
    public void tearDown() throws Exception {
        this.clearAllUsers();
        this.authenticationMetadataManager.shutdown();
    }

    @Test
    public void evaluate1() {
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
        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("DJRRXBXlCVuKh6ULoN87847QX+Y=");
        Assert.assertThrows(AuthenticationException.class, ()-> this.evaluator.evaluate(context));
    }

    @Test
    public void evaluate3() {
        User user = User.of("test", "test");
        this.authenticationMetadataManager.createUser(user);

        DefaultAuthenticationContext context = new DefaultAuthenticationContext();
        context.setRpcCode("11");
        context.setUsername("test");
        context.setContent("test".getBytes(StandardCharsets.UTF_8));
        context.setSignature("test");
        Assert.assertThrows(AuthenticationException.class, ()-> this.evaluator.evaluate(context));
    }

    @Test
    public void evaluate4() {
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
        List<User> users = this.authenticationMetadataManager.listUser(null).join();
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        users.forEach(user -> this.authenticationMetadataManager.deleteUser(user.getUsername()).join());
    }
}