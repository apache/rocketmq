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
package org.apache.rocketmq.acl.plug.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.ControllerParametersEntity;
import org.apache.rocketmq.acl.plug.entity.LoginInfo;

public abstract class LoginInfoAclPlugEngine extends AuthenticationInfoManagementAclPlugEngine {

    private Map<String, LoginInfo> loginInfoMap = new ConcurrentHashMap<>();

    public LoginInfoAclPlugEngine(ControllerParametersEntity controllerParametersEntity) {
        super(controllerParametersEntity);
    }

    public LoginInfo getLoginInfo(AccessControl accessControl) {
        LoginInfo loginInfo = loginInfoMap.get(accessControl.getRecognition());
        if (loginInfo == null) {
            AuthenticationInfo authenticationInfo = super.getAccessControl(accessControl);
            if (authenticationInfo != null) {
                loginInfo = new LoginInfo();
                loginInfo.setAuthenticationInfo(authenticationInfo);
                loginInfoMap.put(accessControl.getRecognition(), loginInfo);
            }
        }
        if (loginInfo != null) {
            loginInfo.setOperationTime(System.currentTimeMillis());
        }
        return loginInfo;
    }

    public void deleteLoginInfo(String remoteAddr) {
        loginInfoMap.remove(remoteAddr);
    }

    protected AuthenticationInfo getAuthenticationInfo(AccessControl accessControl,
        AuthenticationResult authenticationResult) {
        LoginInfo loginInfo = getLoginInfo(accessControl);
        if (loginInfo != null && loginInfo.getAuthenticationInfo() != null) {
            return loginInfo.getAuthenticationInfo();
        }
        authenticationResult.setResultString("Login information does not exist, Please check login, password, IP");
        return null;
    }

}
