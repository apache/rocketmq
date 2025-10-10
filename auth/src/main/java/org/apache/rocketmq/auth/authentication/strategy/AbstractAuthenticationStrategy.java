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
package org.apache.rocketmq.auth.authentication.strategy;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public abstract class AbstractAuthenticationStrategy implements AuthenticationStrategy {

    protected final AuthConfig authConfig;
    protected final Set<String> authenticationWhiteSet = new HashSet<>();
    protected final AuthenticationProvider<AuthenticationContext> authenticationProvider;

    public AbstractAuthenticationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        this.authenticationProvider = AuthenticationFactory.getProvider(authConfig);
        if (this.authenticationProvider != null) {
            this.authenticationProvider.initialize(authConfig, metadataService);
        }
        if (StringUtils.isNotBlank(authConfig.getAuthenticationWhitelist())) {
            String[] whitelist = StringUtils.split(authConfig.getAuthenticationWhitelist(), ",");
            for (String rpcCode : whitelist) {
                this.authenticationWhiteSet.add(StringUtils.trim(rpcCode));
            }
        }
    }

    protected void doEvaluate(AuthenticationContext context) {
        if (context == null) {
            return;
        }
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        if (this.authenticationProvider == null) {
            return;
        }
        if (this.authenticationWhiteSet.contains(context.getRpcCode())) {
            return;
        }
        try {
            this.authenticationProvider.authenticate(context).join();
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            Throwable exception = ExceptionUtils.getRealException(ex);
            if (exception instanceof AuthenticationException) {
                throw (AuthenticationException) exception;
            }
            throw new AuthenticationException("Authentication failed. Please verify the credentials and try again.", exception);
        }
    }
}
