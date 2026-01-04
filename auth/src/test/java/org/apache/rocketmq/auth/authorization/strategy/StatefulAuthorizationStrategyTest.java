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
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.action.Action;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StatefulAuthorizationStrategyTest {

    @Mock
    private AuthConfig authConfig;

    private StatefulAuthorizationStrategy statefulAuthorizationStrategy;

    @Before
    public void setUp() {
        when(authConfig.getStatefulAuthorizationCacheExpiredSecond()).thenReturn(60);
        when(authConfig.getStatefulAuthorizationCacheMaxNum()).thenReturn(100);
        Supplier<?> metadataService = mock(Supplier.class);
        statefulAuthorizationStrategy = spy(new StatefulAuthorizationStrategy(authConfig, metadataService));
    }

    @Test
    public void testEvaluateChannelIdBlankDoesNotUseCache() {
        AuthorizationContext context = mock(AuthorizationContext.class);
        when(context.getChannelId()).thenReturn(null);
        statefulAuthorizationStrategy.evaluate(context);
        verify(statefulAuthorizationStrategy, times(1)).doEvaluate(context);
    }

    @Test
    public void testEvaluateChannelIdNotNullCacheHit() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setChannelId("channelId");
        context.setSubject(Subject.of("User"));
        context.setResource(Resource.of("Cluster"));
        context.setActions(new ArrayList<>());
        context.setSourceIp("sourceIp");
        Pair<Boolean, AuthorizationException> pair = Pair.of(true, null);
        Cache<String, Pair<Boolean, AuthorizationException>> authCache = Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .maximumSize(100)
                .build();
        authCache.put(buildKey(context), pair);
        statefulAuthorizationStrategy.authCache = authCache;
        statefulAuthorizationStrategy.evaluate(context);
        verify(statefulAuthorizationStrategy, never()).doEvaluate(context);
    }

    @Test
    public void testEvaluateChannelIdNotNullCacheMiss() {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setChannelId("channelId");
        context.setSubject(Subject.of("User"));
        context.setResource(Resource.of("Cluster"));
        context.setActions(Collections.singletonList(Action.PUB));
        context.setSourceIp("sourceIp");
        statefulAuthorizationStrategy.authCache = Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .maximumSize(100)
                .build();
        statefulAuthorizationStrategy.evaluate(context);
        verify(statefulAuthorizationStrategy, times(1)).doEvaluate(context);
    }

    @Test
    public void testEvaluateChannelIdNotNullCacheException() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        DefaultAuthorizationContext context = new DefaultAuthorizationContext();
        context.setChannelId("channelId");
        context.setSubject(Subject.of("subjectKey"));
        context.setResource(Resource.of("resourceKey"));
        context.setActions(Collections.singletonList(Action.PUB));
        context.setSourceIp("sourceIp");
        AuthorizationException exception = new AuthorizationException("test");
        Pair<Boolean, AuthorizationException> pair = Pair.of(false, exception);
        Cache<String, Pair<Boolean, AuthorizationException>> authCache = Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .maximumSize(100)
                .build();
        authCache.put(buildKey(context), pair);
        statefulAuthorizationStrategy.authCache = authCache;
        try {
            statefulAuthorizationStrategy.evaluate(context);
            fail("Expected AuthorizationException to be thrown");
        } catch (final AuthorizationException ex) {
            assertEquals(exception, ex);
        }
        verify(statefulAuthorizationStrategy, never()).doEvaluate(context);
    }

    private String buildKey(AuthorizationContext context) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return (String) MethodUtils.invokeMethod(statefulAuthorizationStrategy, true, "buildKey", context);
    }
}
