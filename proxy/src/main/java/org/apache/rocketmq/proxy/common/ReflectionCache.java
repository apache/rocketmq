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

package org.apache.rocketmq.proxy.common;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

public class ReflectionCache {
    private final Cache<Class<?>, Field> fieldCache;
    private static final int DEFAULT_MAX_SIZE = 15;

    public ReflectionCache() {
        this(DEFAULT_MAX_SIZE);
    }

    public ReflectionCache(int maxSize) {
        this.fieldCache = CacheBuilder.newBuilder().maximumSize(maxSize).expireAfterAccess(5, TimeUnit.MINUTES).build();
    }

    public Field getDeclaredField(final Class<?> clazz, final String fieldName) throws Exception {
        return this.fieldCache.get(clazz, () -> {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        });
    }
}

