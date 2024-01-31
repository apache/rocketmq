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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.CommonConstants;

public class StatefulAuthorizationStrategy extends AbstractAuthorizationStrategy {

    protected Cache<String, Pair<Boolean, AuthorizationException>> authCache;

    public StatefulAuthorizationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        super(authConfig, metadataService);
        this.authCache = Caffeine.newBuilder()
            .expireAfterWrite(authConfig.getStatefulAuthorizationCacheExpiredSecond(), TimeUnit.SECONDS)
            .maximumSize(authConfig.getStatefulAuthorizationCacheMaxNum())
            .build();
    }

    @Override
    public void evaluate(AuthorizationContext context) {
        if (StringUtils.isBlank(context.getChannelId())) {
            this.doEvaluate(context);
            return;
        }
        Pair<Boolean, AuthorizationException> result = this.authCache.get(buildKey(context), key -> {
            try {
                this.doEvaluate(context);
                return Pair.of(true, null);
            } catch (AuthorizationException ex) {
                return Pair.of(false, ex);
            }
        });
        if (result != null && result.getObject1() == Boolean.FALSE) {
            throw result.getObject2();
        }
    }

    private String buildKey(AuthorizationContext context) {
        if (context instanceof DefaultAuthorizationContext) {
            DefaultAuthorizationContext ctx = (DefaultAuthorizationContext) context;
            return ctx.getChannelId()
                + CommonConstants.HYPHEN + ctx.getSubjectKey()
                + CommonConstants.HYPHEN + ctx.getResourceKey()
                + CommonConstants.HYPHEN + StringUtils.join(ctx.getActions(), CommonConstants.COMMA)
                + CommonConstants.HYPHEN + ctx.getSourceIp();
        }
        throw new AuthorizationException("The request of {} is not support.", context.getClass().getSimpleName());
    }
}
