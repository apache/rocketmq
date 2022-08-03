package org.apache.rocketmq.store.ha.netty;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TransferMessage {

    private TransferType type;

    private long epoch;

    private int bodyLength;

    private final List<ByteBuffer> byteBufferList;

    public TransferMessage(TransferType type, long epoch) {
        this.type = type;
        this.epoch = epoch;
        this.byteBufferList = new ArrayList<>(2);
    }

    public TransferType getType() {
        return type;
    }

    public void setType(TransferType type) {
        this.type = type;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public byte[] getBytes() {
        byte[] body = new byte[this.bodyLength];
        for (ByteBuffer byteBuffer : byteBufferList) {
            byteBuffer.get(body);
        }
        return body;
    }

    public ByteBuffer getByteBuffer() {
        return byteBufferList.size() > 0 ? byteBufferList.get(0) : null;
    }

    public List<ByteBuffer> getByteBufferList() {
        return byteBufferList;
    }

    public void appendBody(ByteBuffer byteBuffer) {
        this.byteBufferList.add(byteBuffer);
        this.bodyLength += byteBuffer.remaining();
    }

    public void appendBody(byte[] bytes) {
        this.byteBufferList.add(ByteBuffer.wrap(bytes));
        this.bodyLength += bytes.length;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }
}
