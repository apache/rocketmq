package org.apache.rocketmq.acl.plug;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.entity.LoginOrRequestAccessControl;
import org.apache.rocketmq.acl.plug.strategy.OneNetaddressStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthenticationTest {

    Authentication authentication = new Authentication();

    AuthenticationInfo authenticationInfo;

    @Before
    public void init() {
        OneNetaddressStrategy netaddressStrategy = new OneNetaddressStrategy("127.0.0.1");
        BorkerAccessControl borkerAccessControl = new BorkerAccessControl();
        //321
        borkerAccessControl.setQueryConsumeQueue(false);

        Set<String> permitSendTopic = new HashSet<>();
        permitSendTopic.add("permitSendTopic");
        borkerAccessControl.setPermitSendTopic(permitSendTopic);

        Set<String> noPermitSendTopic = new HashSet<>();
        noPermitSendTopic.add("noPermitSendTopic");
        borkerAccessControl.setNoPermitSendTopic(noPermitSendTopic);

        Set<String> permitPullTopic = new HashSet<>();
        permitPullTopic.add("permitPullTopic");
        borkerAccessControl.setPermitPullTopic(permitPullTopic);

        Set<String> noPermitPullTopic = new HashSet<>();
        noPermitPullTopic.add("noPermitPullTopic");
        borkerAccessControl.setNoPermitPullTopic(noPermitPullTopic);

        AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(borkerAccessControl);

        authenticationInfo = new AuthenticationInfo(map, borkerAccessControl, netaddressStrategy);
    }

    @Test
    public void authenticationTest() {

        AuthenticationResult authenticationResult = new AuthenticationResult();
        LoginOrRequestAccessControl loginOrRequestAccessControl = new LoginOrRequestAccessControl();
        loginOrRequestAccessControl.setCode(317);

        boolean isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setCode(321);
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        loginOrRequestAccessControl.setCode(10);
        loginOrRequestAccessControl.setTopic("permitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setCode(310);
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setCode(320);
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setTopic("noPermitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        loginOrRequestAccessControl.setTopic("nopermitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setCode(11);
        loginOrRequestAccessControl.setTopic("permitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        loginOrRequestAccessControl.setTopic("noPermitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        loginOrRequestAccessControl.setTopic("nopermitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, loginOrRequestAccessControl, authenticationResult);
        Assert.assertTrue(isReturn);

    }
}
