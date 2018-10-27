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

import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;

public class Authentication {

    public boolean authentication(AuthenticationInfo authenticationInfo,
        AccessControl accessControl, AuthenticationResult authenticationResult) {
        int code = accessControl.getCode();
        if (!authenticationInfo.getAuthority().get(code)) {
            authenticationResult.setResultString(String.format("code is %d Authentication failed", code));
            return false;
        }
        if (!(authenticationInfo.getAccessControl() instanceof BorkerAccessControl)) {
            return true;
        }
        BorkerAccessControl borker = (BorkerAccessControl) authenticationInfo.getAccessControl();
        String topicName = accessControl.getTopic();
        if (code == 10 || code == 310 || code == 320) {
            if (borker.getPermitSendTopic().contains(topicName)) {
                return true;
            }
            if (borker.getNoPermitSendTopic().contains(topicName)) {
                authenticationResult.setResultString(String.format("noPermitSendTopic include %s", topicName));
                return false;
            }
            return borker.getPermitSendTopic().isEmpty() ? true : false;
        } else if (code == 11) {
            if (borker.getPermitPullTopic().contains(topicName)) {
                return true;
            }
            if (borker.getNoPermitPullTopic().contains(topicName)) {
                authenticationResult.setResultString(String.format("noPermitPullTopic include %s", topicName));
                return false;
            }
            return borker.getPermitPullTopic().isEmpty() ? true : false;
        }
        return true;
    }
}
