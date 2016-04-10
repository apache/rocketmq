/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-10
 */
public class ConsumeStatsList extends RemotingSerializable {
    private List<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsList = new ArrayList<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>>();
    private String brokerAddr;
    private long totalDiff;

    public List<Map<String, List<ConsumeStats>>> getConsumeStatsList() {
        return consumeStatsList;
    }

    public void setConsumeStatsList(List<Map<String, List<ConsumeStats>>> consumeStatsList) {
        this.consumeStatsList = consumeStatsList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public long getTotalDiff() {
        return totalDiff;
    }

    public void setTotalDiff(long totalDiff) {
        this.totalDiff = totalDiff;
    }
}
