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

package org.apache.rocketmq.test.clientinterface;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.test.util.RandomUtil;
import org.apache.rocketmq.test.util.data.collect.DataCollector;
import org.apache.rocketmq.test.util.data.collect.DataCollectorManager;

public abstract class MQCollector {
    protected DataCollector msgBodys = null;
    protected DataCollector originMsgs = null;
    protected DataCollector errorMsgs = null;
    protected Map<Object, Object> originMsgIndex = null;
    protected Collection<Object> msgBodysCopy = null;

    protected DataCollector msgRTs = null;

    public MQCollector() {
        msgBodys = DataCollectorManager.getInstance()
            .fetchListDataCollector(RandomUtil.getStringByUUID());
        originMsgs = DataCollectorManager.getInstance()
            .fetchListDataCollector(RandomUtil.getStringByUUID());
        errorMsgs = DataCollectorManager.getInstance()
            .fetchListDataCollector(RandomUtil.getStringByUUID());
        originMsgIndex = new ConcurrentHashMap<Object, Object>();
        msgRTs = DataCollectorManager.getInstance()
            .fetchListDataCollector(RandomUtil.getStringByUUID());
    }

    public MQCollector(String originMsgCollector, String msgBodyCollector) {
        originMsgs = DataCollectorManager.getInstance().fetchDataCollector(originMsgCollector);
        msgBodys = DataCollectorManager.getInstance().fetchDataCollector(msgBodyCollector);
    }

    public Collection<Object> getAllMsgBody() {
        return msgBodys.getAllData();
    }

    public Collection<Object> getAllOriginMsg() {
        return originMsgs.getAllData();
    }

    public Object getFirstMsg() {
        return ((List<Object>) originMsgs.getAllData()).get(0);
    }

    public Collection<Object> getAllUndupMsgBody() {
        return msgBodys.getAllDataWithoutDuplicate();
    }

    public Collection<Object> getAllUndupOriginMsg() {
        return originMsgs.getAllData();
    }

    public Collection<Object> getSendErrorMsg() {
        return errorMsgs.getAllData();
    }

    public Collection<Object> getMsgRTs() {
        return msgRTs.getAllData();
    }

    public Map<Object, Object> getOriginMsgIndex() {
        return originMsgIndex;
    }

    public Collection<Object> getMsgBodysCopy() {
        msgBodysCopy = new ArrayList<Object>();
        msgBodysCopy.addAll(msgBodys.getAllData());
        return msgBodysCopy;
    }

    public void clearMsg() {
        if (msgBodys != null) {
            msgBodys.resetData();
        }
        if (originMsgs != null) {
            originMsgs.resetData();
        }
        if (originMsgs != null) {
            errorMsgs.resetData();
        }
        if (originMsgIndex != null) {
            originMsgIndex.clear();
        }
        if (msgRTs != null) {
            msgRTs.resetData();
        }
    }

    public void lockCollectors() {
        msgBodys.lockIncrement();
        originMsgs.lockIncrement();
        errorMsgs.lockIncrement();
        msgRTs.lockIncrement();

    }
}
