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

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.parser.ParserConfig;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.rocketmq.common.fastjson.GenericMapSuperclassDeserializer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Optional.fromNullable;

public class LogicalQueuesInfo extends HashMap<Integer, List<LogicalQueueRouteData>> {
    // TODO whether here needs more fine-grained locks like per logical queue lock?
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // only be set in broker, will be empty in namesrv
    private final Map<Integer, LogicalQueueRouteData> queueId2LogicalQueueMap = new ConcurrentHashMap<Integer, LogicalQueueRouteData>();

    public LogicalQueuesInfo() {
        super();
    }

    public LogicalQueuesInfo(Map<Integer, List<LogicalQueueRouteData>> m) {
        super(m);
    }

    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    public LogicalQueuesInfo makeDeepCopy() {
        return this.makeDeepCopy(null);
    }

    public LogicalQueuesInfo makeDeepCopy(Predicate<LogicalQueueRouteData> predicate) {
        this.readLock().lock();
        try {
            LogicalQueuesInfo copy = new LogicalQueuesInfo();
            for (Map.Entry<Integer, List<LogicalQueueRouteData>> entry : this.entrySet()) {
                List<LogicalQueueRouteData> list = Lists.newArrayListWithCapacity(entry.getValue().size());
                for (LogicalQueueRouteData d : entry.getValue()) {
                    if (predicate != null && !predicate.apply(d)) {
                        continue;
                    }
                    list.add(new LogicalQueueRouteData(d));
                }
                copy.put(entry.getKey(), list);
            }
            return copy;
        } finally {
            this.readLock().unlock();
        }
    }

    public void updateQueueRouteDataByQueueId(int queueId, LogicalQueueRouteData queueRouteData) {
        if (queueRouteData == null) {
            queueId2LogicalQueueMap.remove(queueId);
        } else {
            queueId2LogicalQueueMap.put(queueId, queueRouteData);
        }
    }

    /**
     * find logical queue route data for message queues owned by this broker
     *
     * @param queueId
     * @return
     */
    public LogicalQueueRouteData queryQueueRouteDataByQueueId(int queueId) {
        return queueId2LogicalQueueMap.get(queueId);
    }

    public void updateLogicalQueueRouteDataList(int logicalQueueIdx,
        List<LogicalQueueRouteData> logicalQueueRouteDataList) {
        logicalQueueRouteDataList = new LinkedList<LogicalQueueRouteData>(logicalQueueRouteDataList);
        this.writeLock().lock();
        try {
            List<LogicalQueueRouteData> queueRouteDataList = this.get(logicalQueueIdx);
            for (LogicalQueueRouteData logicalQueueRouteData : queueRouteDataList) {
                for (Iterator<LogicalQueueRouteData> it = logicalQueueRouteDataList.iterator(); it.hasNext(); ) {
                    LogicalQueueRouteData newQueueRouteData = it.next();
                    if (Objects.equal(newQueueRouteData.getMessageQueue(), logicalQueueRouteData.getMessageQueue())) {
                        logicalQueueRouteData.copyFrom(newQueueRouteData);
                        it.remove();
                        break;
                    }
                }
                if (logicalQueueRouteDataList.isEmpty()) {
                    break;
                }
            }
            for (LogicalQueueRouteData queueRouteData : logicalQueueRouteDataList) {
                int idx = Collections.binarySearch(queueRouteDataList, queueRouteData);
                if (idx < 0) {
                    idx = -idx - 1;
                }
                queueRouteDataList.add(idx, queueRouteData);
            }
        } finally {
            this.writeLock().unlock();
        }
    }

    @JSONField(serialize = false)
    public Collection<LogicalQueueRouteData> getAllOwnedLogicalQueueRouteData() {
        return queueId2LogicalQueueMap.values();
    }

    public LogicalQueueRouteData nextAvailableLogicalRouteData(LogicalQueueRouteData queueRouteData,
        Predicate<LogicalQueueRouteData> predicate) {
        this.readLock().lock();
        try {
            List<LogicalQueueRouteData> queueRouteDataList = fromNullable(this.get(queueRouteData.getLogicalQueueIndex())).or(Collections.<LogicalQueueRouteData>emptyList());
            int idx = Collections.binarySearch(queueRouteDataList, queueRouteData);
            if (idx >= 0) {
                for (int i = idx + 1, size = queueRouteDataList.size(); i < size; i++) {
                    LogicalQueueRouteData tmp = queueRouteDataList.get(i);
                    if (predicate.apply(tmp)) {
                        return tmp;
                    }
                }
            }
        } finally {
            this.readLock().unlock();
        }
        return null;
    }

    static {
        // workaround https://github.com/alibaba/fastjson/issues/3730
        ParserConfig.getGlobalInstance().putDeserializer(LogicalQueuesInfo.class, GenericMapSuperclassDeserializer.INSTANCE);
    }
}
