package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.atomic.AtomicLong;

public class PutStatsInfor {
    private String topic;
    private String partition;
    private String cliHostName;
    private AtomicLong succesCount = new AtomicLong(0);
    private AtomicLong faileCount = new AtomicLong(0);
    private AtomicLong timeoutCount= new AtomicLong(0);
    private AtomicLong excTime= new AtomicLong(0);
    private AtomicLong msgSizeSum= new AtomicLong(0);

    public PutStatsInfor(String topic, String partition, String cliHostName,
                         long succesCount, long faileCount, long timeoutCount,
                         long excTime,long msgSize) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.cliHostName = cliHostName;
        this.succesCount.set(succesCount);
        this.faileCount.set(faileCount);
        this.timeoutCount.set(timeoutCount);
        this.excTime.set(excTime);
        this.msgSizeSum.set(msgSize);
    }
    public boolean isNull() {
        return !((succesCount.get()+faileCount.get()+timeoutCount.get()+excTime.get()+msgSizeSum.get())>0);
    }
    public void dispose() {
        succesCount.set(0);
        faileCount.set(0);
        timeoutCount.set(0);
        excTime.set(0);
        msgSizeSum.set(0);
    }
    public String tolog(){
        StringBuffer sb =new StringBuffer();
        sb.append("客户端Put消息执行统计").append(",");
        sb.append("服务端[").append(System.getProperty("HOST_NAME")).append("],");
        sb.append("Topic[").append(topic).append("],");
        sb.append("Partition[").append(partition).append("],");
        sb.append("客户端[").append(cliHostName).append("],");
        sb.append("成功数[").append(succesCount.get()).append("],");
        sb.append("失败数[").append(faileCount.get()).append("],");
        sb.append("超时数[").append(timeoutCount.get()).append("],");
        sb.append("exc时间[").append(excTime.get()).append("],");
        sb.append("消息流量[").append(msgSizeSum.get()).append("]");
        return sb.toString();

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
    public AtomicLong getSuccesCount() {
        return succesCount;
    }
    public AtomicLong getFaileCount() {
        return faileCount;
    }
    public AtomicLong getTimeoutCount() {
        return timeoutCount;
    }
    public AtomicLong getExcTime() {
        return excTime;
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
    public void setSuccesCount(AtomicLong succesCount) {
        this.succesCount = succesCount;
    }
    public void setFaileCount(AtomicLong faileCount) {
        this.faileCount = faileCount;
    }
    public void setTimeoutCount(AtomicLong timeoutCount) {
        this.timeoutCount = timeoutCount;
    }
    public void setExcTime(AtomicLong excTime) {
        this.excTime = excTime;
    }
    public AtomicLong getMsgSizeSum() {
        return msgSizeSum;
    }
    public void setMsgSizeSum(AtomicLong msgSizeSum) {
        this.msgSizeSum = msgSizeSum;
    }


}
