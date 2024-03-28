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

package org.apache.rocketmq.test.util.data.collect.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.test.util.data.collect.DataCollector;

public class MapDataCollectorImpl implements DataCollector {

    private Map<Object, AtomicInteger> datas = new ConcurrentHashMap<Object, AtomicInteger>();
    private boolean lock = false;

    public MapDataCollectorImpl() {

    }

    public MapDataCollectorImpl(Collection<Object> datas) {
        for (Object data : datas) {
            addData(data);
        }
    }

    @Override
    public synchronized void addData(Object data) {
        if (lock) {
            return;
        }
        if (datas.containsKey(data)) {
            datas.get(data).addAndGet(1);
        } else {
            datas.put(data, new AtomicInteger(1));
        }
    }

    @Override
    public Collection<Object> getAllData() {
        List<Object> lst = new ArrayList<Object>();
        for (Entry<Object, AtomicInteger> entry : datas.entrySet()) {
            for (int i = 0; i < entry.getValue().get(); i++) {
                lst.add(entry.getKey());
            }
        }
        return lst;
    }

    @Override
    public long getDataSizeWithoutDuplicate() {
        return datas.keySet().size();
    }

    @Override
    public void resetData() {
        datas.clear();
        unlockIncrement();
    }

    @Override
    public long getDataSize() {
        long sum = 0;
        for (AtomicInteger count : datas.values()) {
            sum = sum + count.get();
        }
        return sum;
    }

    @Override
    public boolean isRepeatedData(Object data) {
        if (datas.containsKey(data)) {
            return datas.get(data).get() == 1;
        }
        return false;
    }

    @Override
    public Collection<Object> getAllDataWithoutDuplicate() {
        return datas.keySet();
    }

    @Override
    public int getRepeatedTimeForData(Object data) {
        if (datas.containsKey(data)) {
            return datas.get(data).intValue();
        }
        return 0;
    }

    @Override
    public void removeData(Object data) {
        datas.remove(data);
    }

    @Override
    public void lockIncrement() {
        lock = true;
    }

    @Override
    public void unlockIncrement() {
        lock = false;
    }
}
