package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class BrokerStatsWrapper extends RemotingSerializable {
    private long msgPutTotalYesterdayMorning;

    private long msgPutTotalTodayMorning;

    private long msgGetTotalYesterdayMorning;

    private long msgGetTotalTodayMorning;

    private long getMessageTransferedMsgCount;

    private Map<String, AtomicLong> putMessageTopicTimesTotal =
            new ConcurrentHashMap<String, AtomicLong>(128);

    public long getMsgPutTotalYesterdayMorning() {
        return msgPutTotalYesterdayMorning;
    }

    public void setMsgPutTotalYesterdayMorning(long msgPutTotalYesterdayMorning) {
        this.msgPutTotalYesterdayMorning = msgPutTotalYesterdayMorning;
    }

    public long getMsgPutTotalTodayMorning() {
        return msgPutTotalTodayMorning;
    }

    public void setMsgPutTotalTodayMorning(long msgPutTotalTodayMorning) {
        this.msgPutTotalTodayMorning = msgPutTotalTodayMorning;
    }

    public long getMsgGetTotalYesterdayMorning() {
        return msgGetTotalYesterdayMorning;
    }

    public void setMsgGetTotalYesterdayMorning(long msgGetTotalYesterdayMorning) {
        this.msgGetTotalYesterdayMorning = msgGetTotalYesterdayMorning;
    }

    public long getMsgGetTotalTodayMorning() {
        return msgGetTotalTodayMorning;
    }

    public void setMsgGetTotalTodayMorning(long msgGetTotalTodayMorning) {
        this.msgGetTotalTodayMorning = msgGetTotalTodayMorning;
    }

    public long getGetMessageTransferedMsgCount() {
        return getMessageTransferedMsgCount;
    }

    public void setGetMessageTransferedMsgCount(long getMessageTransferedMsgCount) {
        this.getMessageTransferedMsgCount = getMessageTransferedMsgCount;
    }

    public Map<String, AtomicLong> getPutMessageTopicTimesTotal() {
        return putMessageTopicTimesTotal;
    }

    public void setPutMessageTopicTimesTotal(Map<String, AtomicLong> putMsgTopicTimesTotal) {
        this.putMessageTopicTimesTotal = putMsgTopicTimesTotal;
    }
}
