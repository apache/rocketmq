package org.apache.rocketmq.common;

public class LogicQueueMappingItem {

    private int gen; //generation, mutable
    private int queueId;
    private String bname;
    private long logicOffset; // the start of the logic offset
    private long startOffset; // the start of the physical offset
    private long timeOfStart = -1; //mutable

    public LogicQueueMappingItem(int gen, int queueId, String bname, long logicOffset, long startOffset, long timeOfStart) {
        this.gen = gen;
        this.queueId = queueId;
        this.bname = bname;
        this.logicOffset = logicOffset;
        this.startOffset = startOffset;
        this.timeOfStart = timeOfStart;
    }

    public long convertToStaticLogicOffset(long physicalLogicOffset) {
        return  logicOffset + (physicalLogicOffset - startOffset);
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
}
