package org.apache.rocketmq.common;

public class LogicQueueMappingItem {

    private int gen; //generation, mutable
    private int queueId;
    private String bname;
    private long logicOffset; // the start of the logic offset
    private long startOffset; // the start of the physical offset
    private long endOffset; // the end of the physical offset
    private long timeOfStart = -1; //mutable

    public LogicQueueMappingItem(int gen, int queueId, String bname, long logicOffset, long startOffset, long timeOfStart) {
        this.gen = gen;
        this.queueId = queueId;
        this.bname = bname;
        this.logicOffset = logicOffset;
        this.startOffset = startOffset;
        this.timeOfStart = timeOfStart;
    }

    public long convertToStaticQueueOffset(long physicalQueueOffset) {
        return  logicOffset + (physicalQueueOffset - startOffset);
    }

    public long convertToPhysicalQueueOffset(long staticQueueOffset) {
        return  (staticQueueOffset - logicOffset) + startOffset;
    }

    public long convertToMaxStaticQueueOffset() {
        if (endOffset >= startOffset) {
            return logicOffset + endOffset - startOffset;
        } else {
            return logicOffset;
        }
    }
    public boolean isShouldDeleted() {
        return endOffset == startOffset;
    }

    public boolean isEndOffsetDecided() {
        //if the endOffset == startOffset, then the item should be deleted
        return endOffset > startOffset;
    }

    public long convertOffsetDelta() {
        return logicOffset - startOffset;
    }

    public int getGen() {
        return gen;
    }

    public void setGen(int gen) {
        this.gen = gen;
    }


    public long getTimeOfStart() {
        return timeOfStart;
    }

    public void setTimeOfStart(long timeOfStart) {
        this.timeOfStart = timeOfStart;
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


    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }
}
