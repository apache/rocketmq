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

package org.apache.rocketmq.ons.api.impl.rocketmq;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.ons.api.Constants;
import org.apache.rocketmq.ons.api.impl.MQClientInfo;
import org.apache.rocketmq.ons.api.impl.authority.exception.AuthenticationException;
import org.apache.rocketmq.ons.api.impl.authority.exception.OnsSessionCredentials;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class OnsClientRPCHook extends AclClientRPCHook {
    private static final int CAL_SIGNATURE_FAILED = 10015;

    public OnsClientRPCHook(SessionCredentials sessionCredentials) {
        super(sessionCredentials);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        try {
            super.doBeforeRequest(remoteAddr, request);
        } catch (AclException aclException) {
            throw new AuthenticationException("CAL_SIGNATURE_FAILED", CAL_SIGNATURE_FAILED, aclException.getMessage(), aclException);
        }
        if (this.getSessionCredentials() instanceof OnsSessionCredentials) {
            OnsSessionCredentials onsSessionCredentials = (OnsSessionCredentials) getSessionCredentials();
            request.addExtField(Constants.ONS_CHANNEL_KEY, onsSessionCredentials.getOnsChannel());
        }
        request.setVersion(MQClientInfo.versionCode);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        super.doAfterResponse(remoteAddr, request, response);
    }

}
