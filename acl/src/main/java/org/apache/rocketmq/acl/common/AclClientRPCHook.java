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
package org.apache.rocketmq.acl.common;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SECURITY_TOKEN;
import static org.apache.rocketmq.acl.common.SessionCredentials.SIGNATURE;

public class AclClientRPCHook implements RPCHook {
    private final SessionCredentialsProvider sessionCredentialsProvider;

    public AclClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentialsProvider = new StaticSessionCredentialsProvider(sessionCredentials);
    }

    public AclClientRPCHook(SessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = sessionCredentialsProvider;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        SessionCredentials sessionCredentials = this.sessionCredentialsProvider.getSessionCredentials();
        // Add AccessKey and SecurityToken into signature calculating.
        request.addExtField(ACCESS_KEY, sessionCredentials.getAccessKey());
        // The SecurityToken value is unnecessary,user can choose this one.
        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SECURITY_TOKEN, sessionCredentials.getSecurityToken());
        }
        byte[] total = AclUtils.combineRequestContent(request, parseRequestContent(request));
        String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(SIGNATURE, signature);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    protected SortedMap<String, String> parseRequestContent(RemotingCommand request) {
        request.makeCustomHeaderToNet();
        Map<String, String> extFields = request.getExtFields();
        // Sort property
        return new TreeMap<>(extFields);
    }

    public SessionCredentials getSessionCredentials() {
        return this.sessionCredentialsProvider.getSessionCredentials();
    }
}
