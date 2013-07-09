package com.alibaba.rocketmq.broker.digestlog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.store.DefaultMessageStore;


public class StoreStatsMoniter {
    static final Log log = LogFactory.getLog(StoreStatsMoniter.class);
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private BrokerController brokerController;
//   

    public StoreStatsMoniter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void tolog() {
        Map<String/* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable = brokerController.getConsumerOffsetManager().getOffsetTable();
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) brokerController.getMessageStore();
        for(String key:offsetTable.keySet()){
            String [] strs = key.split(TOPIC_GROUP_SEPARATOR);
            String topic = strs[0];
            String group = strs[1];
            for(Integer queueId:offsetTable.get(key).keySet()){
                long maxoffsize = defaultMessageStore.getMessageTotalInQueue(topic, queueId);
                StringBuffer sb = new StringBuffer();
                sb.append("客户端Put消息和get消息执行统计").append(",");
                sb.append("Topic[").append(topic).append("],");
                sb.append("Partition[").append(brokerController.getBrokerConfig().getBrokerName()+"-"+queueId).append("],");
                sb.append("PutOffset[").append(maxoffsize).append("],");
                sb.append("消费group[").append(group).append("],");
                sb.append("GetOffset[").append(offsetTable.get(key).get(queueId)).append("]");
                log.info(sb.toString());
            }
            
        }
    }

}
