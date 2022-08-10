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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.test.util.data.collect.DataCollector;

public class ListDataCollectorImpl implements DataCollector {

    private List<Object> datas = new ArrayList<Object>();
    private boolean lock = false;

    public ListDataCollectorImpl() {

    }

    public ListDataCollectorImpl(Collection<Object> datas) {
        for (Object data : datas) {
            addData(data);
        }
    }

    public Collection<Object> getAllData() {
        return datas;
    }

    public synchronized void resetData() {
        datas.clear();
        unlockIncrement();
    }

    public long getDataSizeWithoutDuplicate() {
        return getAllDataWithoutDuplicate().size();
    }

    public synchronized void addData(Object data) {
        if (lock) {
            return;
        }
        datas.add(data);
    }

    public long getDataSize() {
        return datas.size();
    }

    public boolean isRepeatedData(Object data) {
        return Collections.frequency(datas, data) == 1;
    }

    public synchronized Collection<Object> getAllDataWithoutDuplicate() {
        return new HashSet<Object>(datas);
    }

    public int getRepeatedTimeForData(Object data) {
        int res = 0;
        for (Object obj : datas) {
            if (obj.equals(data)) {
                res++;
            }
        }
        return res;
    }

    public synchronized void removeData(Object data) {
        datas.remove(data);
    }

    public void lockIncrement() {
        lock = true;
    }

    public void unlockIncrement() {
        lock = false;
    }
}
