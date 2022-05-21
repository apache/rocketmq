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

package org.apache.rocketmq.thinclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Broker {
    private final String name;
    private final int id;
    private final Endpoints endpoints;

    public Broker(apache.rocketmq.v2.Broker broker) {
        this.name = broker.getName();
        this.id = broker.getId();
        this.endpoints = new Endpoints(broker.getEndpoints());
    }

    public apache.rocketmq.v2.Broker toProtobuf() {
        return apache.rocketmq.v2.Broker.newBuilder().setName(name).setId(id).setEndpoints(endpoints.toProtobuf()).build();
    }

    public String getName() {
        return this.name;
    }

    public int getId() {
        return this.id;
    }

    public Endpoints getEndpoints() {
        return this.endpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Broker broker = (Broker) o;
        return id == broker.id && Objects.equal(name, broker.name) && Objects.equal(endpoints, broker.endpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, id, endpoints);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("id", id)
                .add("endpoints", endpoints)
                .toString();
    }
}