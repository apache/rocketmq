package org.apache.rocketmq.store.delay;

import java.nio.ByteBuffer;

public class DelayIndex {
    public static final int ENTRY_SIZE = 32; // 严格锁死 32 字节

    private final long scheduleTime;
    private final long offset;
    private final int size;
    private final long reversed;
    private final int checksum;

    public DelayIndex(long scheduleTime, long offset, int size, long reversed, int checksum) {
        this.scheduleTime = scheduleTime;
        this.offset = offset;
        this.size = size;
        this.reversed = reversed;
        this.checksum = checksum;
    }

    public byte[] encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(ENTRY_SIZE);
        byteBuffer.putLong(this.scheduleTime);
        byteBuffer.putLong(this.offset);
        byteBuffer.putInt(this.size);
        byteBuffer.putLong(this.reversed);
        byteBuffer.putInt(this.checksum);
        return byteBuffer.array();
    }

    public static DelayIndex decode(ByteBuffer byteBuffer) {
        long scheduleTime = byteBuffer.getLong();
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        long reversed = byteBuffer.getLong();
        int checksum = byteBuffer.getInt();
        return new DelayIndex(scheduleTime, offset, size, reversed, checksum);
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public long getReversed() {
        return reversed;
    }

    public int getChecksum() {
        return checksum;
    }
}
