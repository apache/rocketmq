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

package org.apache.rocketmq.thinclient.message.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

public class Resource {
    private String namespace;
    private String name;

    public Resource(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public Resource(String name) {
        this.namespace = StringUtils.EMPTY;
        this.name = name;
    }

    public Resource(apache.rocketmq.v2.Resource resource) {
        this.namespace = resource.getResourceNamespace();
        this.name = resource.getName();
    }

    public apache.rocketmq.v2.Resource toProtobuf() {
        return apache.rocketmq.v2.Resource.newBuilder().setResourceNamespace(namespace).setName(name).build();
    }

    public String getNamespace() {
        return this.namespace;
    }

    public String getName() {
        return this.name;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Resource resource = (Resource) o;
        return Objects.equal(namespace, resource.namespace) && Objects.equal(name, resource.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(namespace, name);
    }

    @Override
    public String toString() {
        if (!StringUtils.isEmpty(namespace)) {
            return MoreObjects.toStringHelper(this)
                .add("name", this.name)
                .add("namespace", namespace)
                .toString();
        }
        // Output name directly if namespace is empty.
        return name;
    }
}
