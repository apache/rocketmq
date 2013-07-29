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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.store.DefaultMessageStore;


/**
 * 发消息统计
 * 
 * @author 菱叶<jin.qian@alipay.com>
 * @since 2013-7-18
 */
public class PutStatsMoniter {
    private static final Logger log = LoggerFactory.getLogger("PutStatsMoniter");
    private BrokerController brokerController;

    private final Map<String, Long> putMessageTopicTimesTotalLast = new HashMap<String, Long>();
    private final Map<String, Long> putMessageTopicSizeTotalLast = new HashMap<String, Long>();


    public PutStatsMoniter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void tolog() {
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) brokerController.getMessageStore();
        Map<String, AtomicLong> putMessageTopicTimesTotal =
                defaultMessageStore.getStoreStatsService().getPutMessageTopicTimesTotal();
        Map<String, AtomicLong> putMessageTopicSizeTotal =
                defaultMessageStore.getStoreStatsService().getPutMessageTopicSizeTotal();
        for (String topic : putMessageTopicTimesTotal.keySet()) {
            long putMessageTopicTimesTotalValue = putMessageTopicTimesTotal.get(topic).get();
            long putMessageTopicSizeTotalValue = putMessageTopicSizeTotal.get(topic).get();
            long putMessageTopicTimesTotalValueLast =
                    putMessageTopicTimesTotalLast.get(topic) == null ? 0 : putMessageTopicTimesTotalLast
                        .get(topic);
            long putMessageTopicSizeTotalValueLast =
                    putMessageTopicSizeTotalLast.get(topic) == null ? 0 : putMessageTopicSizeTotalLast
                        .get(topic);
            putMessageTopicTimesTotalLast.put(topic, putMessageTopicTimesTotalValue);
            putMessageTopicSizeTotalLast.put(topic, putMessageTopicSizeTotalValue);
            if ((putMessageTopicTimesTotalValue - putMessageTopicTimesTotalValueLast
                    + putMessageTopicSizeTotalValue - putMessageTopicSizeTotalValueLast) > 0) {
                StringBuffer sb = new StringBuffer();
                sb.append("ClientPutCount").append(",");
                sb.append("Topic[").append(topic).append("],");
                sb.append("Total[")
                    .append(putMessageTopicTimesTotalValue - putMessageTopicTimesTotalValueLast).append("],");
                sb.append("TotalSize[")
                    .append(putMessageTopicSizeTotalValue - putMessageTopicSizeTotalValueLast).append("]");
                log.info(sb.toString());
            }
        }
    }

}
