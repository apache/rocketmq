package org.apache.rocketmq.common.statictopic;

public class LogicQueueMappingItem {

    private int gen; //generation, mutable
    private int queueId;
    private String bname;
    private long logicOffset; // the start of the logic offset
    private long startOffset; // the start of the physical offset, included
    private long endOffset = -1; // the end of the physical offset, excluded
    private long timeOfStart = -1; // mutable
    private long timeOfEnd = -1; // mutable

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

    public long computeStaticQueueOffset(long physicalQueueOffset) {
        if (physicalQueueOffset < startOffset) {
            return logicOffset;
        }
        if (endOffset >= startOffset
            && endOffset < physicalQueueOffset) {
            return logicOffset + (endOffset - startOffset);
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
