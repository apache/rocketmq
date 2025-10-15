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
package org.apache.rocketmq.auth.authorization.strategy;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public abstract class AbstractAuthorizationStrategy implements AuthorizationStrategy {

    protected final AuthConfig authConfig;
    protected final Set<String> authorizationWhiteSet = new HashSet<>();
    protected final AuthorizationProvider<AuthorizationContext> authorizationProvider;

    public AbstractAuthorizationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        this.authorizationProvider = AuthorizationFactory.getProvider(authConfig);
        if (this.authorizationProvider != null) {
            this.authorizationProvider.initialize(authConfig, metadataService);
        }
        if (StringUtils.isNotBlank(authConfig.getAuthorizationWhitelist())) {
            String[] whitelist = StringUtils.split(authConfig.getAuthorizationWhitelist(), ",");
            for (String rpcCode : whitelist) {
                this.authorizationWhiteSet.add(StringUtils.trim(rpcCode));
            }
        }
    }

    public void doEvaluate(AuthorizationContext context) {
        if (context == null) {
            return;
        }
        if (!this.authConfig.isAuthorizationEnabled()) {
            return;
        }
        if (this.authorizationProvider == null) {
            return;
        }
        if (this.authorizationWhiteSet.contains(context.getRpcCode())) {
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
            throw new AuthorizationException("Authorization failed. Please verify your access rights and try again.", exception);
        }
    }
}
