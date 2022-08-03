package org.apache.rocketmq.store.ha.protocol;

public class PushCommitLogAck {

    private long confirmOffset;

    private boolean slaveAsyncLearner;

    public PushCommitLogAck(long confirmOffset, boolean slaveAsyncLearner) {
        this.confirmOffset = confirmOffset;
        this.slaveAsyncLearner = slaveAsyncLearner;
    }

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public boolean isSlaveAsyncLearner() {
        return slaveAsyncLearner;
    }

    public void setSlaveAsyncLearner(boolean slaveAsyncLearner) {
        this.slaveAsyncLearner = slaveAsyncLearner;
    }

    @Override
    public String toString() {
        return "PushCommitLogAck{" +
            "confirmOffset=" + confirmOffset +
            ", slaveAsyncLearner=" + slaveAsyncLearner +
            '}';
    }
}
