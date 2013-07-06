package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class StoreStatsInfo {
    static final Log log = LogFactory.getLog(StoreStatsInfo.class);
    private String topic;
    private String partition;
    private AtomicLong putOffset = new AtomicLong(0);
    private ConcurrentHashMap<String/* group */, AtomicLong/* getOffsetCount */> getGroupinfo =
            new ConcurrentHashMap<String, AtomicLong>();


    public StoreStatsInfo(String topic, String partition, long putOffset) {
        this.topic = topic;
        this.partition = partition;
        this.putOffset.set(putOffset);
    }


    public boolean isNull() {
        long count = putOffset.get();
        for (String group : getGroupinfo.keySet()) {
            count = count + getGroupinfo.get(group).get();
        }
        return !(count > 0);
    }


    public void dispose() {

    }


    public void tolog() {
        // List<String> ls = new ArrayList<String>();
        for (String key : getGroupinfo.keySet()) {
            StringBuffer sb = new StringBuffer();
            sb.append("客户端Put消息和get消息执行统计").append(",");
            sb.append("服务端[").append(System.getProperty("HOST_NAME")).append("],");
            sb.append("Topic[").append(topic).append("],");
            sb.append("Partition[").append(partition).append("],");
            sb.append("PutOffset[").append(putOffset.get()).append("],");
            sb.append("消费group[").append(key).append("],");
            sb.append("GetOffset[").append(getGroupinfo.get(key).get()).append("]");
            log.info(sb.toString());
            // ls.add(sb.toString());
        }
        // return ls;
    }


    public void toPutlog() {
        StringBuffer sb = new StringBuffer();
        sb.append("客户端Put消息和get消息执行统计").append(",");
        sb.append("服务端[").append(System.getProperty("HOST_NAME")).append("],");
        sb.append("Topic[").append(topic).append("],");
        sb.append("Partition[").append(partition).append("],");
        sb.append("PutOffset[").append(putOffset.get()).append("],");
        sb.append("消费group[").append("*").append("],");
        sb.append("GetOffset[").append(0).append("]");
        log.info(sb.toString());
        // return sb.toString();
    }


    public String getTopic() {
        return topic;
    }


    public String getPartition() {
        return partition;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public void setPartition(String partition) {
        this.partition = partition;
    }


    public ConcurrentHashMap<String, AtomicLong> getGetGroupinfo() {
        return getGroupinfo;
    }


    public void setGetGroupinfo(ConcurrentHashMap<String, AtomicLong> getGroupinfo) {
        this.getGroupinfo = getGroupinfo;
    }


    public AtomicLong getPutOffset() {
        return putOffset;
    }


    public void setPutOffset(AtomicLong putOffset) {
        this.putOffset = putOffset;
    }
}
