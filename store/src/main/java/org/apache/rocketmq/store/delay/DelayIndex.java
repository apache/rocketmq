package org.apache.rocketmq.store.delay;

import java.nio.ByteBuffer;

public class DelayIndex {
    public static final int ENTRY_SIZE = 32;

    private final long scheduleTime;
    private final long offset;
    private final int size;
    private final long reserved;
    private final int checksum;

    public DelayIndex(long scheduleTime, long offset, int size, long reserved, int checksum) {
        this.scheduleTime = scheduleTime;
        this.offset = offset;
        this.size = size;
        this.reserved = reserved;
        this.checksum = checksum;
    }

    public byte[] encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(ENTRY_SIZE);
        byteBuffer.putLong(this.scheduleTime);
        byteBuffer.putLong(this.offset);
        byteBuffer.putInt(this.size);
        byteBuffer.putLong(this.reserved);
        byteBuffer.putInt(this.checksum);
        return byteBuffer.array();
    }

    public static DelayIndex decode(ByteBuffer byteBuffer) {
        long scheduleTime = byteBuffer.getLong();
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        long reserved = byteBuffer.getLong();
        int checksum = byteBuffer.getInt();
        return new DelayIndex(scheduleTime, offset, size, reserved, checksum);
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

    public long getReserved() {
        return reserved;
    }

    public int getChecksum() {
        return checksum;
    }
}
