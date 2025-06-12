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
package org.apache.rocketmq.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public enum StoreType {
    DEFAULT("default"),
    DEFAULT_ROCKSDB("defaultRocksDB");

    private String storeType;

    StoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getStoreType() {
        return storeType;
    }

    /**
     * convert string to set of StoreType
     *
     * @param str example "default;defaultRocksDB"
     * @return set of StoreType
     */
    public static Set<StoreType> fromString(String str) {
        if (str == null || str.trim().isEmpty()) {
            return Collections.emptySet();
        }

        return Arrays.stream(str.split(";"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> Arrays.stream(StoreType.values())
                .filter(type -> type.getStoreType().equalsIgnoreCase(s))
                .findFirst()
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }
}
