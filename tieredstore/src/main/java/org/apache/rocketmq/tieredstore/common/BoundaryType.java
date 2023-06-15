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
package org.apache.rocketmq.tieredstore.common;

/**
 *  This enumeration represents the boundary types.
 *  It has two constants, lower and upper, which represent the lower and upper boundaries respectively.
 */
public enum BoundaryType {

    /**
     * Represents the lower boundary.
     */
    LOWER("lower"),

    /**
     * Represents the upper boundary.
     */
    UPPER("upper");

    private final String name;

    BoundaryType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static BoundaryType getType(String name) {
        if (BoundaryType.UPPER.getName().equalsIgnoreCase(name)) {
            return UPPER;
        }
        return LOWER;
    }
}
