package org.apache.rocketmq.store;

import java.util.concurrent.CopyOnWriteArrayList;

public class NonPersistentConsumeQueue {
    private String topic;
    private int queueId;
    private long maxOffsetInQueue;
    private long minOffsetInQueue;
    private long maxOffset;
    private long minOffset;

    private final CopyOnWriteArrayList<NonPersistentMsg> msgCopyOnWriteArrayList = new CopyOnWriteArrayList<NonPersistentMsg>();

    private NonPersistentMessageStore nonPersistentMessageStore;

    public NonPersistentConsumeQueue() {
    }

//    public NonPersistentConsumeQueue(NonPersistentMessageStore msgStore){
//        this.nonPersistentMessageStore = msgStore;
//    }


    public NonPersistentMessageStore getNonPersistentMessageStore() {
        return nonPersistentMessageStore;
    }


    public NonPersistentConsumeQueue(String topic, int queueId, NonPersistentMessageStore msgStore){
        this.queueId = queueId;
        this.topic = topic;
        this.nonPersistentMessageStore = msgStore;
    }

    public PutMessageResult putMessage(MessageExtBrokerInner msg){
        AppendMessageResult result = null;
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
        return putMessageResult;
    }

    public CopyOnWriteArrayList<NonPersistentMsg> getMsgCopyOnWriteArrayList() {
        return msgCopyOnWriteArrayList;
    }

    public long getMaxOffsetInQueue(){
        return maxOffsetInQueue;
    }

    public long getMinOffsetInQueue() {
        return minOffsetInQueue;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    //根据offset获取msg
    public SelectMappedBufferResult getMessageResult(long offset){
        NonPersistentMsg msg = msgCopyOnWriteArrayList.get((int)offset);
        SelectMappedBufferResult selectedMappedBufferResult = new SelectMappedBufferResult(offset, msg);
        return selectedMappedBufferResult;
    }

    public SelectMappedBufferResult getMessage(long offset, int size){
        SelectMappedBufferResult selectedMappedBufferResult = new SelectMappedBufferResult(offset, size);
        return selectedMappedBufferResult;
    }
}
