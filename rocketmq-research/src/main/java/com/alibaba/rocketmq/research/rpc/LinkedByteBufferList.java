/**
 * $Id: LinkedByteBufferList.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 针对写优化的ByteBuffer序列
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class LinkedByteBufferList {
    class ByteBufferNode {
        public static final int NODE_SIZE = 1024 * 1024 * 4;
        private final AtomicInteger writeOffset = new AtomicInteger(0);
        private ByteBuffer byteBufferWrite;
        private ByteBuffer byteBufferRead;
        private volatile ByteBufferNode nextByteBufferNode;


        public ByteBufferNode clearAndReturnNew() {
            this.writeOffset.set(0);
            this.byteBufferWrite.position(0);
            this.byteBufferWrite.limit(NODE_SIZE);
            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(NODE_SIZE);
            this.nextByteBufferNode = null;
            return this;
        }


        public boolean isReadable() {
            return this.byteBufferRead.position() < this.writeOffset.get();
        }


        public boolean isReadover() {
            return this.byteBufferRead.position() == NODE_SIZE;
        }


        private ByteBuffer allocateNewByteBuffer(final int size) {
            return ByteBuffer.allocate(size);
        }


        public ByteBufferNode() {
            LinkedByteBufferList.this.nodeTotal++;
            this.nextByteBufferNode = null;
            this.byteBufferWrite = allocateNewByteBuffer(NODE_SIZE);
            this.byteBufferRead = this.byteBufferWrite.slice();
        }


        public ByteBuffer getByteBufferWrite() {
            return byteBufferWrite;
        }


        public void setByteBufferWrite(ByteBuffer byteBufferWrite) {
            this.byteBufferWrite = byteBufferWrite;
        }


        public ByteBuffer getByteBufferRead() {
            return byteBufferRead;
        }


        public void setByteBufferRead(ByteBuffer byteBufferRead) {
            this.byteBufferRead = byteBufferRead;
        }


        public ByteBufferNode getNextByteBufferNode() {
            return nextByteBufferNode;
        }


        public void setNextByteBufferNode(ByteBufferNode nextByteBufferNode) {
            this.nextByteBufferNode = nextByteBufferNode;
        }


        public AtomicInteger getWriteOffset() {
            return writeOffset;
        }
    }

    private volatile int nodeTotal = 0;
    private ByteBufferNode currentWriteNode;
    private ByteBufferNode currentReadNode;
    private final LinkedBlockingDeque<ByteBufferNode> bbnIdleList =
            new LinkedBlockingDeque<LinkedByteBufferList.ByteBufferNode>();

    // 是否已经被Notify过
    protected volatile boolean hasNotified = false;


    public LinkedByteBufferList() {
        this.currentWriteNode = new ByteBufferNode();
        this.currentReadNode = this.currentWriteNode;
    }


    // TODO 可能需要流控
    public void putData(final int reqId, final byte[] data) {
        final int HEADER_SIZE = 8;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(data.length);
        header.putInt(reqId);
        header.flip();
        synchronized (this) {
            int minHeader = Math.min(HEADER_SIZE, this.currentWriteNode.getByteBufferWrite().remaining());
            int minData = 0;
            // 尝试写入头
            if (minHeader > 0) {
                this.currentWriteNode.getByteBufferWrite().put(header.array(), 0, minHeader);
                this.currentWriteNode.getWriteOffset().addAndGet(minHeader);
            }
            // 尝试写入体
            if (minHeader == HEADER_SIZE) {
                minData = Math.min(data.length, this.currentWriteNode.getByteBufferWrite().remaining());
                if (minData > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(data, 0, minData);
                    this.currentWriteNode.getWriteOffset().addAndGet(minData);
                }
            }

            // 需要创建新的Buffer
            if (!this.currentWriteNode.getByteBufferWrite().hasRemaining()) {
                ByteBufferNode newNode = null;
                // 尝试从空闲处取
                newNode = this.bbnIdleList.poll();
                if (null == newNode) {
                    newNode = new ByteBufferNode();
                }

                this.currentWriteNode.setNextByteBufferNode(newNode.clearAndReturnNew());
                this.currentWriteNode = newNode;

                // 补偿Header
                int remainHeaderPut = HEADER_SIZE - minHeader;
                int remainDataPut = data.length - minData;
                if (remainHeaderPut > 0) {
                    this.currentWriteNode.getByteBufferWrite()
                        .put(header.array(), minHeader, remainHeaderPut);
                    this.currentWriteNode.getWriteOffset().addAndGet(remainHeaderPut);
                }

                // 补偿Data
                if (remainDataPut > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(data, minData, remainDataPut);
                    this.currentWriteNode.getWriteOffset().addAndGet(remainDataPut);
                }
            }

            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }


    public ByteBufferNode findReadableNode() {
        if (this.getCurrentReadNode().isReadable()) {
            return this.getCurrentReadNode();
        }

        if (this.getCurrentReadNode().isReadover()) {
            if (this.getCurrentReadNode().getNextByteBufferNode() != null) {
                this.bbnIdleList.add(this.getCurrentReadNode());
                this.setCurrentReadNode(this.getCurrentReadNode().getNextByteBufferNode());
                return this.getCurrentReadNode();
            }
        }

        return null;
    }


    public ByteBufferNode waitForPut(long interval) {
        ByteBufferNode found = this.findReadableNode();
        if (found != null) {
            return found;
        }

        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                found = this.findReadableNode();
                if (found != null) {
                    return found;
                }
            }

            try {
                this.wait(interval);
                return this.findReadableNode();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
            }
        }

        return null;
    }


    public ByteBufferNode getCurrentReadNode() {
        return currentReadNode;
    }


    public void setCurrentReadNode(ByteBufferNode currentReadNode) {
        this.currentReadNode = currentReadNode;
    }


    public LinkedBlockingDeque<ByteBufferNode> getBbnIdleList() {
        return bbnIdleList;
    }


    public int getNodeTotal() {
        return nodeTotal;
    }
}
