package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.atomic.AtomicLong;

public class GetMissStatsInfo {
    private String topic;
    private String partition;
    private String cliHostName;
    private String group;
    private AtomicLong count = new AtomicLong(0);
    public GetMissStatsInfo(String topic, String partition, String cliHostName, String group,
                            long count) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.cliHostName = cliHostName;
        this.group = group;
        this.count.set(count);
    }
    public void dispose() {
        count.set(0);
    }
    public String tolog(){
        StringBuffer sb =new StringBuffer();
        sb.append("客户端GetMiss执行统计").append(",");
        sb.append("服务端[").append(System.getProperty("HOST_NAME")).append("],");
        sb.append("Topic[").append(topic).append("],");
        sb.append("Partition[").append(partition).append("],");
        sb.append("客户端[").append(cliHostName).append("],");
        sb.append("Group[").append(group).append("],");
        sb.append("Count[").append(count.get()).append("]");
        return sb.toString();

    }
    public boolean isNull() {
        return !(count.get()>0);
    }
    public String getTopic() {
        return topic;
    }
    public String getPartition() {
        return partition;
    }
    public String getCliHostName() {
        return cliHostName;
    }
    public String getGroup() {
        return group;
    }
    public AtomicLong getCount() {
        return count;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public void setPartition(String partition) {
        this.partition = partition;
    }
    public void setCliHostName(String cliHostName) {
        this.cliHostName = cliHostName;
    }
    public void setGroup(String group) {
        this.group = group;
    }
    public void setCount(AtomicLong count) {
        this.count = count;
    }

}
