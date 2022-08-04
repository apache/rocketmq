package org.apache.rocketmq.store.ha.protocol;

public class ConfirmTruncate {

    private boolean syncFromLastFile;

    private Long commitLogStartOffset;

    public ConfirmTruncate(boolean syncFromLastFile, Long commitLogStartOffset) {
        this.syncFromLastFile = syncFromLastFile;
        this.commitLogStartOffset = commitLogStartOffset;
    }

    public boolean isSyncFromLastFile() {
        return syncFromLastFile;
    }

    public void setSyncFromLastFile(boolean syncFromLastFile) {
        this.syncFromLastFile = syncFromLastFile;
    }

    public Long getCommitLogStartOffset() {
        return commitLogStartOffset;
    }

    public void setCommitLogStartOffset(Long commitLogStartOffset) {
        this.commitLogStartOffset = commitLogStartOffset;
    }

    @Override
    public String toString() {
        return "ConfirmTruncate{" +
            "syncFromLastFile=" + syncFromLastFile +
            ", commitLogStartOffset=" + commitLogStartOffset +
            '}';
    }
}
