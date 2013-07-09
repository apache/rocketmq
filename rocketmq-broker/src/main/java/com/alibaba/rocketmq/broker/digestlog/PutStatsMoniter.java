package com.alibaba.rocketmq.broker.digestlog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.store.DefaultMessageStore;


public class PutStatsMoniter {
    static final Log log = LogFactory.getLog(PutStatsMoniter.class);
    private BrokerController brokerController;
    
    private final Map <String,Long> putMessageTopicTimesTotalLast = new HashMap<String,Long>();
    private final Map <String,Long> putMessageTopicSizeTotalLast = new HashMap<String,Long>();
    
    public PutStatsMoniter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void tolog() {
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) brokerController.getMessageStore();
        Map <String,AtomicLong> putMessageTopicTimesTotal = defaultMessageStore.getStoreStatsService().getPutMessageTopicTimesTotal();
        Map <String,AtomicLong> putMessageTopicSizeTotal = defaultMessageStore.getStoreStatsService().getPutMessageTopicSizeTotal();
        for(String topic:putMessageTopicTimesTotalLast.keySet()){
            long putMessageTopicTimesTotalValue = putMessageTopicTimesTotal.get(topic).get();
            long putMessageTopicSizeTotalValue = putMessageTopicSizeTotal.get(topic).get();
            long putMessageTopicTimesTotalValueLast =putMessageTopicTimesTotalLast.get(topic)==null?0:putMessageTopicTimesTotalLast.get(topic);
            long putMessageTopicSizeTotalValueLast = putMessageTopicSizeTotalLast.get(topic)==null?0:putMessageTopicSizeTotalLast.get(topic);
            putMessageTopicTimesTotalLast.put(topic, putMessageTopicTimesTotalValue);
            putMessageTopicSizeTotalLast.put(topic, putMessageTopicSizeTotalValue);
            StringBuffer sb = new StringBuffer();
            sb.append("客户端Put消息执行统计").append(",");
            sb.append("Topic[").append(topic).append("],");
            sb.append("成功数[").append(putMessageTopicTimesTotalValue-putMessageTopicTimesTotalValueLast).append("],");
            sb.append("消息流量[").append(putMessageTopicSizeTotalValue-putMessageTopicSizeTotalValueLast).append("]");
            log.info(sb.toString());
        }
    }

}
