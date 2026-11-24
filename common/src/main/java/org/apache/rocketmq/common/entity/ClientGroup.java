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

package org.apache.rocketmq.common.entity;

import java.util.Objects;

public class ClientGroup {

    public final String clientId;
    public final String group;
    /**
     * Cache the hash code for the object
     */
    private int hash; // Default to 0

    public ClientGroup(String clientId, String group) {
        this.clientId = clientId;
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientGroup that = (ClientGroup) o;
        return Objects.equals(clientId, that.clientId)
            && Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Objects.hash(clientId, group);
        }
        return hash;
    }

    @Override
    public String toString() {
        return "ClientGroup{" +
            "clientId='" + clientId + '\'' +
            ", group='" + group + '\'' +
            '}';
    }
}
