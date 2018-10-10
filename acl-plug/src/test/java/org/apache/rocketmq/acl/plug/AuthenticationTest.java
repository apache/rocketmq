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
package org.apache.rocketmq.acl.plug;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategyFactory;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AuthenticationTest {

    Authentication authentication = new Authentication();

    AuthenticationInfo authenticationInfo;

    BorkerAccessControl borkerAccessControl;

    AuthenticationResult authenticationResult = new AuthenticationResult();
    AccessControl accessControl = new AccessControl();

    @Before
    public void init() {
        borkerAccessControl = new BorkerAccessControl();
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
        accessContralAnalysis.analysisClass(RequestCode.class);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(borkerAccessControl);

        authenticationInfo = new AuthenticationInfo(map, borkerAccessControl, NetaddressStrategyFactory.NULL_NET_ADDRESS_STRATEGY);
    }

    @Test
    public void authenticationTest() {

        accessControl.setCode(317);

        boolean isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(321);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setCode(10);
        accessControl.setTopic("permitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(310);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(320);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setTopic("noPermitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setTopic("nopermitSendTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setCode(11);
        accessControl.setTopic("permitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setTopic("noPermitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        accessControl.setTopic("nopermitPullTopic");
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

    }

    @Test
    public void isEmptyTest() {
        accessControl.setCode(10);
        accessControl.setTopic("absentTopic");
        boolean isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        Set<String> permitSendTopic = new HashSet<>();
        borkerAccessControl.setPermitSendTopic(permitSendTopic);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);

        accessControl.setCode(11);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertFalse(isReturn);

        borkerAccessControl.setPermitPullTopic(permitSendTopic);
        isReturn = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
        Assert.assertTrue(isReturn);
    }

}
