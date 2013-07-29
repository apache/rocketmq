/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.digestlog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.store.DefaultMessageStore;


/**
 * 拉消息统计
 * 
 * @author 菱叶<jin.qian@alipay.com>
 * @since 2013-7-18
 */
public class GetStatsMoniter {
    private static final Logger log = LoggerFactory.getLogger("GetStatsMoniter");
    private BrokerController brokerController;
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private Map<String, HashMap<Integer, Long>> offsetTableLast =
            new HashMap<String, HashMap<Integer, Long>>();


    public GetStatsMoniter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void tolog() {
        Map<String/* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable =
                brokerController.getConsumerOffsetManager().getOffsetTable();
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) brokerController.getMessageStore();
        for (String key : offsetTable.keySet()) {
            String[] strs = key.split(TOPIC_GROUP_SEPARATOR);
            String topic = strs[0];
            String group = strs[1];
            for (Integer queueId : offsetTable.get(key).keySet()) {
                long nowvalue = offsetTable.get(key).get(queueId);
                long lastvalue = getLastValue(key, queueId, nowvalue);
                offsetTableLast.get(key).put(queueId, nowvalue);
                if ((nowvalue - lastvalue) > 0) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("ClientGetConut").append(",");
                    sb.append("Topic[").append(topic).append("],");
                    sb.append("Mq[")
                        .append(brokerController.getBrokerConfig().getBrokerName() + "-" + queueId)
                        .append("],");
                    sb.append("Group[").append(group).append("],");
                    sb.append("Total[").append(nowvalue - lastvalue).append("]");
                    log.info(sb.toString());
                }

            }

        }
    }


    private long getLastValue(String key, Integer queueId, long nowValue) {
        if (this.offsetTableLast.get(key) == null) {
            this.offsetTableLast.put(key, new HashMap());
            this.offsetTableLast.get(key).put(queueId, nowValue);
            return 0;
        }
        else if (this.offsetTableLast.get(key).get(queueId) == null) {
            offsetTableLast.get(key).put(queueId, nowValue);
            return 0;
        }
        return this.offsetTableLast.get(key).get(queueId);
    }

}
