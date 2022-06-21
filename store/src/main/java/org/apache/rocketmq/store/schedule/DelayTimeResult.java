
package org.apache.rocketmq.store.schedule;

import java.util.List;

public class DelayTimeResult {
    List<DelayTimeItem> delayTimeItemList;

    public List<DelayTimeItem> getDelayTimeItemList() {
        return delayTimeItemList;
    }

    public void setDelayTimeItemList(List<DelayTimeItem> delayTimeItemList) {
        this.delayTimeItemList = delayTimeItemList;
    }

    public static class DelayTimeItem{
       long phyOffset;
       int msgSize;
       public long getPhyOffset() {
           return phyOffset;
       }

       public void setPhyOffset(long phyOffset) {
           this.phyOffset = phyOffset;
       }

       public int getMsgSize() {
           return msgSize;
       }

       public void setMsgSize(int msgSize) {
           this.msgSize = msgSize;
       }
   }
}