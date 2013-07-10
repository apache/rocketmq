package com.alibaba.rocketmq.broker.digestlog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.store.DefaultMessageStore;


public class GetStatsMoniter {
    static final Log log = LogFactory.getLog("GetStatsMoniter");
    private BrokerController brokerController;
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private Map<String, HashMap<Integer, Long>> offsetTableLast=new HashMap<String, HashMap<Integer, Long>>();
    public GetStatsMoniter(BrokerController brokerController) {
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
                long nowvalue = offsetTable.get(key).get(queueId);
                long lastvalue = getLastValue(key,queueId,nowvalue);
                if((nowvalue-lastvalue)>0){
                    StringBuffer sb = new StringBuffer();
                    sb.append("ClientGetConut").append(",");
                    sb.append("Topic[").append(topic).append("],");
                    sb.append("Partition[").append(brokerController.getBrokerConfig().getBrokerName()+"-"+queueId).append("],");
                    sb.append("Group[").append(group).append(",");
                    sb.append("Total[").append(nowvalue-lastvalue).append("]");
                    log.info(sb.toString());
                }
            }
            
        }
    }
    private long getLastValue(String key ,Integer queueId,long nowValue){
        if(this.offsetTableLast.get(key)==null){
            this.offsetTableLast.put(key, new HashMap());
            this.offsetTableLast.get(key).put(queueId, nowValue);
            return 0 ;
        }else if(this.offsetTableLast.get(key).get(queueId)==null){
            offsetTableLast.get(key).put(queueId, nowValue);
            return 0 ;
        }
        return this.offsetTableLast.get(key).get(queueId) ;
    }

}
