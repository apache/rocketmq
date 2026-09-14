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
package org.apache.rocketmq.auth.authorization;

import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.strategy.AuthorizationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationEvaluator {

    private final AuthorizationStrategy authorizationStrategy;

    public AuthorizationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthorizationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authorizationStrategy = AuthorizationFactory.getStrategy(authConfig, metadataService);
    }

    /**
     * Visible for testing: allows injecting a stub strategy.
     */
    AuthorizationEvaluator(AuthorizationStrategy authorizationStrategy) {
        this.authorizationStrategy = authorizationStrategy;
    }

    public void evaluate(List<? extends AuthorizationContext> contexts) {
        if (CollectionUtils.isEmpty(contexts)) {
            throw new AuthorizationException("authorization context is empty.");
        }
        contexts.forEach(this.authorizationStrategy::evaluate);
    }

    public void evaluate(RemotingCommand request, List<? extends AuthorizationContext> contexts) {
        if (CollectionUtils.isNotEmpty(contexts)) {
            contexts.forEach(this.authorizationStrategy::evaluate);
            return;
        }
        if (!AuthorizationCompatibility.matches(request)) {
            throw new AuthorizationException("authorization context is empty.");
        }
    }

    public void evaluate(GeneratedMessageV3 request, List<? extends AuthorizationContext> contexts) {
        if (CollectionUtils.isNotEmpty(contexts)) {
            contexts.forEach(this.authorizationStrategy::evaluate);
            return;
        }
        if (!AuthorizationCompatibility.matches(request)) {
            throw new AuthorizationException("authorization context is empty.");
        }
    }
}
