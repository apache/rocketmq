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

package org.apache.rocketmq.remoting.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents a generic type {@code T}. Java doesn't yet provide a way to
 * represent generic types, so this class does. Forces clients to create a
 * subclass of this class which enables retrieval the type information even at
 * runtime.
 *
 * <p>For example, to create a type literal for {@code List<String>}, you can
 * create an empty anonymous inner class:
 *
 * <pre>
 * TypePresentation&lt;List&lt;String&gt;&gt; list = new TypePresentation&lt;List&lt;String&gt;&gt;() {};
 * </pre>
 *
 * To create a type literal for {@code Map<String, Integer>}:
 *
 * <pre>
 * TypePresentation&lt;Map&lt;String, Integer&gt;&gt; map = new TypePresentation&lt;Map&lt;String, Integer&gt;&gt;() {};
 * </pre>
 *
 * This syntax cannot be used to create type literals that have wildcard
 * parameters, such as {@code Class<?>} or {@code List<? extends CharSequence>}.
 *
 * @since 1.0.0
 */
public class TypePresentation<T> {
    static ConcurrentMap<Class<?>, ConcurrentMap<Type, ConcurrentMap<Type, Type>>> classTypeCache
        = new ConcurrentHashMap<Class<?>, ConcurrentMap<Type, ConcurrentMap<Type, Type>>>(16, 0.75f, 1);
    protected final Type type;

    /**
     * Constructs a new type literal. Derives represented class from type
     * parameter.
     *
     * <p>Clients create an empty anonymous subclass. Doing so embeds the type
     * parameter in the anonymous class's type hierarchy so we can reconstitute it
     * at runtime despite erasure.
     */
    protected TypePresentation() {
        Type superClass = getClass().getGenericSuperclass();
        type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    /**
     * @return underlying {@code Type} instance.
     */
    public Type getType() {
        return type;
    }
}
