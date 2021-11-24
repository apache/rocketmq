package org.apache.rocketmq.common.statictopic;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class LogicQueueMappingItem extends RemotingSerializable {

    private int gen; // immutable
    private int queueId; //, immutable
    private String bname; //important, immutable
    private long logicOffset; // the start of the logic offset, important, can be changed by command only once
    private long startOffset; // the start of the physical offset, should always be 0, immutable
    private long endOffset = -1; // the end of the physical offset, excluded, revered -1, mutable
    private long timeOfStart = -1; // mutable, reserved
    private long timeOfEnd = -1; // mutable, reserved

    //make sure it has a default constructor
    public LogicQueueMappingItem() {

    }

    public LogicQueueMappingItem(int gen, int queueId, String bname, long logicOffset, long startOffset, long endOffset, long timeOfStart, long timeOfEnd) {
        this.gen = gen;
        this.queueId = queueId;
        this.bname = bname;
        this.logicOffset = logicOffset;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.timeOfStart = timeOfStart;
        this.timeOfEnd = timeOfEnd;
    }

    public long computeStaticQueueOffsetUpToEnd(long physicalQueueOffset) {
        if (physicalQueueOffset < startOffset) {
            return logicOffset;
        }
        if (endOffset >= startOffset
            && endOffset < physicalQueueOffset) {
            return logicOffset + (endOffset - startOffset);
        }
        return  logicOffset + (physicalQueueOffset - startOffset);
    }

    public long computeStaticQueueOffset(long physicalQueueOffset) {
        if (physicalQueueOffset < startOffset) {
            return logicOffset;
        }
        return  logicOffset + (physicalQueueOffset - startOffset);
    }

    public long computePhysicalQueueOffset(long staticQueueOffset) {
        return  (staticQueueOffset - logicOffset) + startOffset;
    }

    public long computeMaxStaticQueueOffset() {
        if (endOffset >= startOffset) {
            return logicOffset + endOffset - startOffset;
        } else {
            return logicOffset;
        }
    }
    public boolean checkIfShouldDeleted() {
        return endOffset == startOffset;
    }

    public boolean checkIfEndOffsetDecided() {
        //if the endOffset == startOffset, then the item should be deleted
        return endOffset > startOffset;
    }

    public long computeOffsetDelta() {
        return logicOffset - startOffset;
    }

    public int getGen() {
        return gen;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getBname() {
        return bname;
    }

    public long getLogicOffset() {
        return logicOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getTimeOfStart() {
        return timeOfStart;
    }

    public long getTimeOfEnd() {
        return timeOfEnd;
    }

    public void setLogicOffset(long logicOffset) {
        this.logicOffset = logicOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public void setTimeOfStart(long timeOfStart) {
        this.timeOfStart = timeOfStart;
    }

    public void setTimeOfEnd(long timeOfEnd) {
        this.timeOfEnd = timeOfEnd;
    }

    public void setGen(int gen) {
        this.gen = gen;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof LogicQueueMappingItem)) return false;

        LogicQueueMappingItem item = (LogicQueueMappingItem) o;

        return new EqualsBuilder()
                .append(gen, item.gen)
                .append(queueId, item.queueId)
                .append(logicOffset, item.logicOffset)
                .append(startOffset, item.startOffset)
                .append(endOffset, item.endOffset)
                .append(timeOfStart, item.timeOfStart)
                .append(timeOfEnd, item.timeOfEnd)
                .append(bname, item.bname)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(gen)
                .append(queueId)
                .append(bname)
                .append(logicOffset)
                .append(startOffset)
                .append(endOffset)
                .append(timeOfStart)
                .append(timeOfEnd)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "LogicQueueMappingItem{" +
                "gen=" + gen +
                ", queueId=" + queueId +
                ", bname='" + bname + '\'' +
                ", logicOffset=" + logicOffset +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", timeOfStart=" + timeOfStart +
                ", timeOfEnd=" + timeOfEnd +
                '}';
    }
}
