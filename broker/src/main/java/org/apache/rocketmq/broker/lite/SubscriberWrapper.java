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

package org.apache.rocketmq.broker.lite;

import org.apache.rocketmq.common.entity.ClientGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SubscriberWrapper {

    public static class ListWrapper extends SubscriberWrapper {
        private final List<ClientGroup> clients;

        public ListWrapper() {
            this.clients = new ArrayList<>();
        }

        public ListWrapper(List<ClientGroup> clients) {
            this.clients = clients;
        }

        public List<ClientGroup> getClients() {
            return this.clients;
        }
    }

    public static class MapWrapper extends SubscriberWrapper {
        private final Map<String, List<ClientGroup>> groupMap = new HashMap<>();

        public MapWrapper() {
        }

        public Map<String, List<ClientGroup>> getGroupMap() {
            return groupMap;
        }
    }

    public ListWrapper asListWrapper() {
        return this instanceof ListWrapper ? (ListWrapper) this : null;
    }

    public MapWrapper asMapWrapper() {
        return this instanceof MapWrapper ? (MapWrapper) this : null;
    }

}
