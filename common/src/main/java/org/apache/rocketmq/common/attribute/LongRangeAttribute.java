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

public class LongRangeAttribute extends Attribute {
    private final long min;
    private final long max;
    private final long defaultValue;

    public LongRangeAttribute(String name, boolean changeable, long min, long max, long defaultValue) {
        super(name, changeable);
        this.min = min;
        this.max = max;
        this.defaultValue = defaultValue;
    }

    @Override
    public void verify(String value) {
        long l = Long.parseLong(value);
        if (l < min || l > max) {
            throw new RuntimeException(format("value is not in range(%d, %d)", min, max));
        }
    }

    public long getDefaultValue() {
        return defaultValue;
    }
}
