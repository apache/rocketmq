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

import java.util.Set;

public class EnumAttribute extends Attribute {
    private final Set<String> universe;
    private final String defaultValue;

    public EnumAttribute(String name, boolean changeable, Set<String> universe, String defaultValue) {
        super(name, changeable);
        this.universe = universe;
        this.defaultValue = defaultValue;
    }

    @Override
    public void verify(String value) {
        if (!this.universe.contains(value)) {
            throw new RuntimeException("value is not in set: " + this.universe);
        }
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
