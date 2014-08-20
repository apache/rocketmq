package com.alibaba.rocketmq.tools.admin.api;

public class MessageTrack {
    private String consumerGroup;
    private TrackType trackType;
    private String exceptionDesc;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public TrackType getTrackType() {
        return trackType;
    }


    public void setTrackType(TrackType trackType) {
        this.trackType = trackType;
    }


    public String getExceptionDesc() {
        return exceptionDesc;
    }


    public void setExceptionDesc(String exceptionDesc) {
        this.exceptionDesc = exceptionDesc;
    }


    @Override
    public String toString() {
        return "MessageTrack [consumerGroup=" + consumerGroup + ", trackType=" + trackType
                + ", exceptionDesc=" + exceptionDesc + "]";
    }
}
