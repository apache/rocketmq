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
package org.apache.rocketmq.acl;

import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plug.AccessControl;
import org.apache.rocketmq.acl.plug.AclPlugRuntimeException;
import org.apache.rocketmq.acl.plug.AuthenticationResult;
import org.apache.rocketmq.acl.plug.PlainAclPlugEngine;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class PlainAccessValidator implements AccessValidator {

    private PlainAclPlugEngine aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainAclPlugEngine();
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        HashMap<String, String> extFields = request.getExtFields();
        AccessControl accessControl = new AccessControl();
        accessControl.setCode(request.getCode());
        accessControl.setRecognition(remoteAddr);
        accessControl.setNetaddress(StringUtils.split(remoteAddr, ":")[0]);
        if (extFields != null) {
            accessControl.setAccount(extFields.get("account"));
            accessControl.setPassword(extFields.get("password"));
            accessControl.setTopic(extFields.get("topic"));
        }
        return accessControl;
    }

    @Override
    public void validate(AccessResource accessResource) {
    	AuthenticationResult authenticationResult = null;
        try {
             authenticationResult = aclPlugEngine.eachCheckAuthentication((AccessControl) accessResource);
        } catch (Exception e) {
            throw new AclPlugRuntimeException(String.format("validate exception AccessResource data %s", accessResource.toString()), e);
        }
        if (authenticationResult.getException() != null) {
            throw new AclPlugRuntimeException(String.format("eachCheck the inspection appear exception, accessControl data is %s", accessResource.toString()), authenticationResult.getException());
        }
        if (authenticationResult.getAccessControl() == null || !authenticationResult.isSucceed()) {
            throw new AclPlugRuntimeException(String.format("%s accessControl data is %s", authenticationResult.getResultString(), accessResource.toString()));
        }
    }

}
