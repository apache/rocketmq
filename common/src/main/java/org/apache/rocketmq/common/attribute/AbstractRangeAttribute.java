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
package org.apache.rocketmq.common.attribute;

import static java.lang.String.format;

public abstract class AbstractRangeAttribute<T extends Comparable<T>> extends Attribute {

    protected final T min;
    protected final T max;
    protected final T defaultValue;

    public AbstractRangeAttribute(String name, boolean changeable, T min, T max, T defaultValue) {
        super(name, changeable);
        this.min = min;
        this.max = max;
        this.defaultValue = defaultValue;
    }

    protected abstract T parse(String value);

    @Override
    public void verify(String value) {
        T l = parse(value);
        if (l.compareTo(min) < 0 || l.compareTo(max) > 0) {
            throw new RuntimeException(format("value is not in range(%s, %s)", min, max));
        }
    }

    public T getDefaultValue() {
        return defaultValue;
    }
}
