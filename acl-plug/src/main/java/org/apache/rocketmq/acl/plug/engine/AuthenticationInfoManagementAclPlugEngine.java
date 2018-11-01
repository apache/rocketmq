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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.acl.plug.AccessContralAnalysis;
import org.apache.rocketmq.acl.plug.Authentication;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationInfo;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControlTransport;
import org.apache.rocketmq.acl.plug.entity.ControllerParameters;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;
import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategy;
import org.apache.rocketmq.acl.plug.strategy.NetaddressStrategyFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class AuthenticationInfoManagementAclPlugEngine implements AclPlugEngine {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ACL_PLUG_LOGGER_NAME);
    ControllerParameters controllerParameters;
    private Map<String/** account **/, List<AuthenticationInfo>> accessControlMap = new HashMap<>();
    private AuthenticationInfo authenticationInfo;
    private NetaddressStrategyFactory netaddressStrategyFactory = new NetaddressStrategyFactory();
    private AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
    private Authentication authentication = new Authentication();

    public AuthenticationInfoManagementAclPlugEngine(ControllerParameters controllerParameters) {
        this.controllerParameters = controllerParameters;
        accessContralAnalysis.analysisClass(controllerParameters.getAccessContralAnalysisClass());
    }

    public void setAccessControl(AccessControl accessControl) throws AclPlugRuntimeException {
        if (accessControl.getAccount() == null || accessControl.getPassword() == null || accessControl.getAccount().length() <= 6 || accessControl.getPassword().length() <= 6) {
            throw new AclPlugRuntimeException(String.format("The account password cannot be null and is longer than 6, account is %s  password is %s", accessControl.getAccount(), accessControl.getPassword()));
        }
        try {
            NetaddressStrategy netaddressStrategy = netaddressStrategyFactory.getNetaddressStrategy(accessControl);
            List<AuthenticationInfo> accessControlAddressList = accessControlMap.get(accessControl.getAccount());
            if (accessControlAddressList == null) {
                accessControlAddressList = new ArrayList<>();
                accessControlMap.put(accessControl.getAccount(), accessControlAddressList);
            }
            AuthenticationInfo authenticationInfo = new AuthenticationInfo(accessContralAnalysis.analysis(accessControl), accessControl, netaddressStrategy);
            accessControlAddressList.add(authenticationInfo);
            log.info("authenticationInfo is {}", authenticationInfo.toString());
        } catch (Exception e) {
            throw new AclPlugRuntimeException(String.format("Exception info %s  %s", e.getMessage(), accessControl.toString()), e);
        }
    }

    public void setAccessControlList(List<AccessControl> accessControlList) throws AclPlugRuntimeException {
        for (AccessControl accessControl : accessControlList) {
            setAccessControl(accessControl);
        }
    }

    public void setNetaddressAccessControl(AccessControl accessControl) throws AclPlugRuntimeException {
        try {
            authenticationInfo = new AuthenticationInfo(accessContralAnalysis.analysis(accessControl), accessControl, netaddressStrategyFactory.getNetaddressStrategy(accessControl));
            log.info("default authenticationInfo is {}", authenticationInfo.toString());
        } catch (Exception e) {
            throw new AclPlugRuntimeException(accessControl.toString(), e);
        }

    }

    public AuthenticationInfo getAccessControl(AccessControl accessControl) {
        if (accessControl.getAccount() == null && authenticationInfo != null) {
            return authenticationInfo.getNetaddressStrategy().match(accessControl) ? authenticationInfo : null;
        } else {
            List<AuthenticationInfo> accessControlAddressList = accessControlMap.get(accessControl.getAccount());
            if (accessControlAddressList != null) {
                for (AuthenticationInfo ai : accessControlAddressList) {
                    if (ai.getNetaddressStrategy().match(accessControl) && ai.getAccessControl().getPassword().equals(accessControl.getPassword())) {
                        return ai;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public AuthenticationResult eachCheckLoginAndAuthentication(AccessControl accessControl) {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        try {
            AuthenticationInfo authenticationInfo = getAuthenticationInfo(accessControl, authenticationResult);
            if (authenticationInfo != null) {
                boolean boo = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
                authenticationResult.setSucceed(boo);
                authenticationResult.setAccessControl(authenticationInfo.getAccessControl());
            }
        } catch (Exception e) {
            authenticationResult.setException(e);
        }
        return authenticationResult;
    }

    public AuthenticationResult eachCheckAuthentication(AccessControl accessControl) {
        AuthenticationResult authenticationResult = new AuthenticationResult();
        AuthenticationInfo authenticationInfo = getAccessControl(accessControl);
        if (authenticationInfo != null) {
            boolean boo = authentication.authentication(authenticationInfo, accessControl, authenticationResult);
            authenticationResult.setSucceed(boo);
            authenticationResult.setAccessControl(authenticationInfo.getAccessControl());
        } else {
            authenticationResult.setResultString("accessControl is null, Please check login, password, IP\"");
        }


        return authenticationResult;
    }

    void setBorkerAccessControlTransport(BorkerAccessControlTransport transport) {
        if (transport.getOnlyNetAddress() == null && (transport.getList() == null || transport.getList().size() == 0)) {
            throw new AclPlugRuntimeException("onlyNetAddress and list  can't be all empty");
        }

        if (transport.getOnlyNetAddress() != null) {
            this.setNetaddressAccessControl(transport.getOnlyNetAddress());
        }
        if (transport.getList() != null || transport.getList().size() > 0) {
            for (AccessControl accessControl : transport.getList()) {
                this.setAccessControl(accessControl);
            }
        }
    }

    protected abstract AuthenticationInfo getAuthenticationInfo(AccessControl accessControl,
                                                                AuthenticationResult authenticationResult);
}
