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

    private List<Object> dataList = new ArrayList<Object>();
    private boolean lock = false;

    public ListDataCollectorImpl() {

    }

    public ListDataCollectorImpl(Collection<Object> dataList) {
        for (Object data : dataList) {
            addData(data);
        }
    }

    public Collection<Object> getAllData() {
        return dataList;
    }

    public void resetData() {
        dataList.clear();
        unlockIncrement();
    }

    public long getDataSizeWithoutDuplicate() {
        return getAllDataWithoutDuplicate().size();
    }

    public synchronized void addData(Object data) {
        if (lock) {
            return;
        }
        dataList.add(data);
    }

    public long getDataSize() {
        return dataList.size();
    }

    public boolean isRepeatedData(Object data) {
        return Collections.frequency(dataList, data) == 1;
    }

    public Collection<Object> getAllDataWithoutDuplicate() {
        return new HashSet<Object>(dataList);
    }

    public int getRepeatedTimeForData(Object data) {
        int res = 0;
        for (Object obj : dataList) {
            if (obj.equals(data)) {
                res++;
            }
        }
        return res;
    }

    public void removeData(Object data) {
        dataList.remove(data);
    }

    public void lockIncrement() {
        lock = true;
    }

    public void unlockIncrement() {
        lock = false;
    }
}
