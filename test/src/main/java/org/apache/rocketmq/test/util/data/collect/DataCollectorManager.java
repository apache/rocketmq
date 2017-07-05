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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util.data.collect;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.test.util.data.collect.impl.ListDataCollectorImpl;
import org.apache.rocketmq.test.util.data.collect.impl.MapDataCollectorImpl;

public final class DataCollectorManager {
    private static DataCollectorManager instance = new DataCollectorManager();
    private Map<String, DataCollector> collectMap = new HashMap<String, DataCollector>();
    private Object lock = new Object();

    private DataCollectorManager() {
    }

    public static DataCollectorManager getInstance() {
        return instance;
    }

    public DataCollector fetchDataCollector(String key) {
        String realKey = key;
        if (!collectMap.containsKey(realKey)) {
            synchronized (lock) {
                if (!collectMap.containsKey(realKey)) {
                    DataCollector collect = (DataCollector) new MapDataCollectorImpl();
                    collectMap.put(realKey, collect);
                }
            }
        }
        return collectMap.get(realKey);
    }

    public DataCollector fetchMapDataCollector(String key) {
        String realKey = key;
        if (!collectMap.containsKey(realKey)
            || collectMap.get(realKey) instanceof ListDataCollectorImpl) {
            synchronized (lock) {
                if (!collectMap.containsKey(realKey)
                    || collectMap.get(realKey) instanceof ListDataCollectorImpl) {
                    DataCollector collect = null;
                    if (collectMap.containsKey(realKey)) {
                        DataCollector src = collectMap.get(realKey);
                        collect = new MapDataCollectorImpl(src.getAllData());
                    } else {
                        collect = new MapDataCollectorImpl();
                    }
                    collectMap.put(realKey, collect);

                }
            }
        }
        return collectMap.get(realKey);
    }

    public DataCollector fetchListDataCollector(String key) {
        String realKey = key;
        if (!collectMap.containsKey(realKey)
            || collectMap.get(realKey) instanceof MapDataCollectorImpl) {
            synchronized (lock) {
                if (!collectMap.containsKey(realKey)
                    || collectMap.get(realKey) instanceof MapDataCollectorImpl) {
                    DataCollector collect = null;
                    if (collectMap.containsKey(realKey)) {
                        DataCollector src = collectMap.get(realKey);
                        collect = new ListDataCollectorImpl(src.getAllData());
                    } else {
                        collect = new ListDataCollectorImpl();
                    }
                    collectMap.put(realKey, collect);
                }
            }
        }
        return collectMap.get(realKey);
    }

    public void resetDataCollect(String key) {
        if (collectMap.containsKey(key)) {
            collectMap.get(key).resetData();
        }
    }

    public void resetAll() {
        for (Map.Entry<String, DataCollector> entry : collectMap.entrySet()) {
            entry.getValue().resetData();
        }
    }

    public void removeDataCollect(String key) {
        collectMap.remove(key);
    }

    public void removeAll() {
        collectMap.clear();
    }
}
