/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * HA服务，Master用来向Slave Push数据，并接收Slave应答
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class HAConnection {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private final HAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddr;
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;
    // Slave请求从哪里开始拉数据
    private volatile long slaveRequestOffset = -1;
    // Slave收到数据后，应答Offset
    private volatile long slaveAckOffset = -1;


    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }


    /**
     * 向Slave传输数据协议 <Phy Offset> <Body Size> <Body Data><br>
     * 从Slave接收数据协议 <Phy Offset>
     */

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }


    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }


    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            }
            catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }


    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 读取Slave请求，一般为push ack
     * 
     * @author shijia.wxr<vintage.wang@gmail.com>
     */
    class ReadSocketService extends ServiceThread {
        private static final int ReadMaxBufferSize = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(ReadMaxBufferSize);
        private int processPostion = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();


        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            // 线程自动回收，不需要被其他线程join
            this.thread.setDaemon(true);
        }


        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    // 检测心跳间隔时间，超过则强制断开
                    long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now()
                                    - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore()
                        .getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr
                                + "] expired, " + interval);
                        break;
                    }
                }
                catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            // 避免内存泄露
            haService.removeConnection(HAConnection.this);

            // 只有读线程需要执行
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            }
            catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }


        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPostion = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp =
                                HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        // 接收Slave上传的offset
                        if ((this.byteBufferRead.position() - this.processPostion) >= 8) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPostion = pos;

                            // 处理Slave的请求
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset "
                                        + readOffset);
                            }

                            // 通知前端线程
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    }
                    else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    }
                    else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                }
                catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }


        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }
    }

    /**
     * 向Slave写入数据
     * 
     * @author shijia.wxr<vintage.wang@gmail.com>
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;
        // 要传输的数据
        private final int HEADER_SIZE = 8 + 4;
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(HEADER_SIZE);
        private long nextTransferFromWhere = -1;
        private SelectMapedBufferResult selectMapedBufferResult;
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();


        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.thread.setDaemon(true);
        }


        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);

                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 第一次传输，需要计算从哪里开始
                    // Slave如果本地没有数据，请求的Offset为0，那么master则从物理文件最后一个文件开始传送数据
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset =
                                    HAConnection.this.haService.getDefaultMessageStore().getCommitLog()
                                        .getMaxOffset();
                            masterOffset =
                                    masterOffset
                                            - (masterOffset % HAConnection.this.haService
                                                .getDefaultMessageStore().getMessageStoreConfig()
                                                .getMapedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        }
                        else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave["
                                + HAConnection.this.clientAddr + "], and slave request "
                                + HAConnection.this.slaveRequestOffset);
                    }

                    if (this.lastWriteOver) {
                        // 如果长时间没有发消息则尝试发心跳
                        long interval =
                                HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now()
                                        - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore()
                            .getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                            // 向Slave发送心跳
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(HEADER_SIZE);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    }
                    // 继续传输
                    else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 传输数据,
                    // selectResult会赋值给this.selectMapedBufferResult，出现异常也会清理掉
                    SelectMapedBufferResult selectResult =
                            HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(
                                this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore()
                            .getMessageStoreConfig().getHaTransferBatchSize()) {
                            size =
                                    HAConnection.this.haService.getDefaultMessageStore()
                                        .getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMapedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(HEADER_SIZE);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    }
                    else {
                        // 没有数据，等待通知
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                }
                catch (Exception e) {
                    // 只要抛出异常，一般是网络发生错误，连接必须断开，并清理资源
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 清理资源
            if (this.selectMapedBufferResult != null) {
                this.selectMapedBufferResult.release();
            }

            this.makeStop();

            // 避免内存泄露
            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            }
            catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }


        /**
         * 表示是否传输完成
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                }
                else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                }
                else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMapedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMapedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMapedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp =
                                HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    }
                    else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    }
                    else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result =
                    !this.byteBufferHeader.hasRemaining()
                            && !this.selectMapedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMapedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMapedBufferResult.release();
                this.selectMapedBufferResult = null;
            }

            return result;
        }


        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }


        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
