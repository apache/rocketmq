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

import org.apache.rocketmq.acl.plug.engine.AclPlugEngine;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.AuthenticationResult;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;

public class DefaultAclRemotingServerImpl implements AclRemotingServer {

    private AclPlugEngine aclPlugEngine;

    public DefaultAclRemotingServerImpl(AclPlugEngine aclPlugEngine) {
        this.aclPlugEngine = aclPlugEngine;
    }

    @Override
    public AuthenticationResult check(AccessControl accessControl) {
        AuthenticationResult authenticationResult = aclPlugEngine.eachCheckLoginAndAuthentication(accessControl);
        if (authenticationResult.getException() != null) {
            throw new AclPlugRuntimeException(String.format("eachCheck the inspection appear exception, accessControl data is %s", accessControl.toString()), authenticationResult.getException());
        }
        if (authenticationResult.getAccessControl() == null) {
            throw new AclPlugRuntimeException(String.format("%s accessControl data is %s", authenticationResult.getResultString(), accessControl.toString()));
        }
        if (!authenticationResult.isSucceed()) {
            throw new AclPlugRuntimeException(String.format("%s accessControl data is %s", authenticationResult.getResultString(), accessControl.toString()));
        }
        return authenticationResult;
    }

}
