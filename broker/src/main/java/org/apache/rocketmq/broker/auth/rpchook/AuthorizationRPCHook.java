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
package org.apache.rocketmq.broker.auth.rpchook;

import java.util.List;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationRPCHook implements RPCHook {

    private final AuthConfig authConfig;

    private final AuthorizationEvaluator evaluator;

    public AuthorizationRPCHook(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthorizationFactory.getEvaluator(authConfig);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (this.evaluator == null) {
            return;
        }
        if (authConfig.isAuthorizationEnabled()) {
            List<AuthorizationContext> contexts = AuthorizationFactory.newContexts(this.authConfig, request, remoteAddr);
            this.evaluator.evaluate(contexts);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }
}
