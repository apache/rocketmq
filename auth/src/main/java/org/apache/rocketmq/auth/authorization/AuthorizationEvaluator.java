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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthorizationEvaluator {

    private final AuthConfig authConfig;
    private final List<String> authorizationWhitelist = new ArrayList<>();
    private final AuthorizationProvider<AuthorizationContext> authorizationProvider;

    public AuthorizationEvaluator(AuthConfig authConfig) {
        this(authConfig, null);
    }

    public AuthorizationEvaluator(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        this.authorizationProvider = AuthorizationFactory.getProvider(authConfig);
        if (this.authorizationProvider != null) {
            this.authorizationProvider.initialize(authConfig, metadataService);
        }
        if (StringUtils.isNotBlank(authConfig.getAuthorizationWhitelist())) {
            String[] whitelist = StringUtils.split(authConfig.getAuthorizationWhitelist(), ",");
            for (String rpcCode : whitelist) {
                this.authorizationWhitelist.add(StringUtils.trim(rpcCode));
            }
        }
    }

    public void evaluate(List<AuthorizationContext> contexts) {
        if (CollectionUtils.isEmpty(contexts)) {
            return;
        }
        contexts.forEach(this::evaluate);
    }

    public void evaluate(AuthorizationContext context) {
        if (context == null) {
            return;
        }
        if (!this.authConfig.isAuthorizationEnabled()) {
            return;
        }
        if (this.authorizationProvider == null) {
            return;
        }
        if (this.authorizationWhitelist.contains(context.getRpcCode())) {
            return;
        }
        try {
            this.authorizationProvider.authorize(context).join();
        } catch (AuthorizationException ex) {
            throw ex;
        } catch (Throwable ex) {
            Throwable exception = ExceptionUtils.getRealException(ex);
            if (exception instanceof AuthorizationException) {
                throw (AuthorizationException) exception;
            }
            throw new AuthorizationException("failed to authorization the request", exception);
        }
    }
}
