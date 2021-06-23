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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.fastjson.GenericMapSuperclassDeserializer;

public class LogicalQueuesInfo extends HashMap<Integer, List<LogicalQueueRouteData>> {
    // TODO whether here needs more fine-grained locks like per logical queue lock?
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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

    public void updateLogicalQueueRouteDataList(int logicalQueueIdx,
        List<LogicalQueueRouteData> logicalQueueRouteDataList) {
        this.writeLock().lock();
        try {
            logicalQueueRouteDataList = Lists.newLinkedList(logicalQueueRouteDataList);
            List<LogicalQueueRouteData> queueRouteDataList = this.get(logicalQueueIdx);
            for (LogicalQueueRouteData logicalQueueRouteData : queueRouteDataList) {
                for (Iterator<LogicalQueueRouteData> it = logicalQueueRouteDataList.iterator(); it.hasNext(); ) {
                    LogicalQueueRouteData newQueueRouteData = it.next();
                    if (Objects.equal(newQueueRouteData.getMessageQueue(), logicalQueueRouteData.getMessageQueue()) && newQueueRouteData.getOffsetDelta() == logicalQueueRouteData.getOffsetDelta()) {
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

    static {
        // workaround https://github.com/alibaba/fastjson/issues/3730
        ParserConfig.getGlobalInstance().putDeserializer(LogicalQueuesInfo.class, GenericMapSuperclassDeserializer.INSTANCE);
    }
}
