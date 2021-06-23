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
package org.apache.rocketmq.common.protocol.route;

import com.alibaba.fastjson.parser.ParserConfig;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.rocketmq.common.fastjson.GenericMapSuperclassDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Only used inside namesrv, between client and namesrv, to reduce cpu usage of namesrv
 */
public class LogicalQueuesInfoUnordered extends ConcurrentHashMap<Integer, Map<LogicalQueuesInfoUnordered.Key, LogicalQueueRouteData>> {
    static {
        // workaround https://github.com/alibaba/fastjson/issues/3730
        ParserConfig.getGlobalInstance().putDeserializer(LogicalQueuesInfoUnordered.class, GenericMapSuperclassDeserializer.INSTANCE);
    }

    public LogicalQueuesInfoUnordered() {
        super();
    }

    public LogicalQueuesInfoUnordered(int size) {
        super(size);
    }

    public LogicalQueuesInfo toLogicalQueuesInfoOrdered() {
        LogicalQueuesInfo logicalQueuesInfoOrdered = new LogicalQueuesInfo();
        for (Map.Entry<Integer, Map<Key, LogicalQueueRouteData>> entry : this.entrySet()) {
            List<LogicalQueueRouteData> list = Lists.newArrayListWithExpectedSize(entry.getValue().size());
            for (LogicalQueueRouteData d : entry.getValue().values()) {
                list.add(new LogicalQueueRouteData(d));
            }
            Collections.sort(list);
            logicalQueuesInfoOrdered.put(entry.getKey(), list);
        }
        return logicalQueuesInfoOrdered;
    }

    public static class Key {
        private final String brokerName;
        private final int queueId;

        private final long offsetDelta;

        private final int hash;

        public Key(String brokerName, int queueId, long offsetDelta) {
            this.brokerName = brokerName;
            this.queueId = queueId;
            this.offsetDelta = offsetDelta;

            this.hash = Objects.hashCode(brokerName, queueId, this.offsetDelta);
        }

        public String getBrokerName() {
            return brokerName;
        }

        public int getQueueId() {
            return queueId;
        }

        public long getOffsetDelta() {
            return offsetDelta;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key id = (Key) o;
            return queueId == id.queueId && offsetDelta == id.offsetDelta && Objects.equal(brokerName, id.brokerName);
        }

        @Override public int hashCode() {
            return hash;
        }

        @Override public String toString() {
            return "Key{" +
                "brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                ", offsetDelta=" + offsetDelta +
                '}';
        }
    }
}
