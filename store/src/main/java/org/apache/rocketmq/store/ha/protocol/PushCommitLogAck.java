package org.apache.rocketmq.store.ha.protocol;

public class PushCommitLogAck {

    private long confirmOffset = 0L;

    private boolean isReadOnly = false;

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }

    @Override
    public String toString() {
        return "PushCommitLogAck{" +
            "confirmOffset=" + confirmOffset +
            ", isReadOnly=" + isReadOnly +
            '}';
    }
}
