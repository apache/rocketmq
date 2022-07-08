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

import static com.google.common.base.Preconditions.checkNotNull;

public class BooleanAttribute extends Attribute {
    private final boolean defaultValue;

    public BooleanAttribute(String name, boolean changeable, boolean defaultValue) {
        super(name, changeable);
        this.defaultValue = defaultValue;
    }

    @Override
    public void verify(String value) {
        checkNotNull(value);

        if (!"false".equalsIgnoreCase(value) && !"true".equalsIgnoreCase(value)) {
            throw new RuntimeException("boolean attribute format is wrong.");
        }
    }

    public boolean getDefaultValue() {
        return defaultValue;
    }
}
