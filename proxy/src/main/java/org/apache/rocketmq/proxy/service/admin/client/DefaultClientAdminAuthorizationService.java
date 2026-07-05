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
package org.apache.rocketmq.proxy.service.admin.client;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;

public class DefaultClientAdminAuthorizationService implements ClientAdminAuthorizationService {
    private final AuthConfig authConfig;
    private final ClientAdminAuthPolicy authPolicy;
    private final AuthorizationEvaluator authorizationEvaluator;

    public DefaultClientAdminAuthorizationService(AuthConfig authConfig, Supplier<?> metadataService) {
        this(
            authConfig,
            new ClientAdminAuthPolicy(),
            authConfig == null ? null : AuthorizationFactory.getEvaluator(authConfig, metadataService)
        );
    }

    DefaultClientAdminAuthorizationService(AuthConfig authConfig, ClientAdminAuthPolicy authPolicy,
        AuthorizationEvaluator authorizationEvaluator) {
        this.authConfig = authConfig;
        this.authPolicy = authPolicy;
        this.authorizationEvaluator = authorizationEvaluator;
    }

    @Override
    public void authorize(Subject subject, ClientAdminOperation operation, String sourceIp) {
        if (authConfig == null || !authConfig.isAuthorizationEnabled()) {
            return;
        }
        if (subject == null) {
            throw new AuthorizationException("subject is required");
        }
        DefaultAuthorizationContext context = authPolicy.newContext(
            subject,
            operation,
            authConfig.getClusterName(),
            sourceIp
        );
        List<AuthorizationContext> contexts = Collections.singletonList(context);
        authorizationEvaluator.evaluate(contexts);
    }
}
