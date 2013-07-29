/**
 * $Id: Connection.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.research.rpc.LinkedByteBufferList.ByteBufferNode;


/**
 * 一个Socket连接对象，Client与Server通用
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Connection {
    private static final int ReadMaxBufferSize = 1024 * 1024 * 4;

    private final SocketChannel socketChannel;
    private final RPCProcessor rpcServerProcessor;
    private final ThreadPoolExecutor executor;
    private final LinkedByteBufferList linkeByteBufferList = new LinkedByteBufferList();
    private int dispatchPostion = 0;
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(ReadMaxBufferSize);

    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;

    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;


        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = Selector.open();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        }


        @Override
        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);
                    int writeSizeZeroTimes = 0;
                    while (true) {
                        ByteBufferNode node = Connection.this.linkeByteBufferList.waitForPut(100);
                        if (node != null) {
                            node.getByteBufferRead().limit(node.getWriteOffset().get());
                            int writeSize = this.socketChannel.write(node.getByteBufferRead());
                            if (writeSize > 0) {
                            }
                            else if (writeSize == 0) {
                                if (++writeSizeZeroTimes >= 3) {
                                    break;
                                }
                            }
                            else {
                            }
                        }
                        else {
                            break;
                        }
                    }
                }
                catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                    break;
                }
            }

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(this.getServiceName() + " service end");
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

    class ReadSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;


        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = Selector.open();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
        }


        @Override
        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = Connection.this.processReadEvent();
                    if (!ok) {
                        System.out.println("processReadEvent error");
                        break;
                    }
                }
                catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                    break;
                }
            }

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }
    }


    public Connection(final SocketChannel socketChannel, final RPCProcessor rpcServerProcessor,
            final ThreadPoolExecutor executor) {
        this.socketChannel = socketChannel;
        this.rpcServerProcessor = rpcServerProcessor;
        this.executor = executor;

        try {
            this.socketChannel.configureBlocking(false);
            this.socketChannel.socket().setSoLinger(false, -1);
            this.socketChannel.socket().setTcpNoDelay(true);
            this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
            this.socketChannel.socket().setSendBufferSize(1024 * 64);
            this.writeSocketService = new WriteSocketService(this.socketChannel);
            this.readSocketService = new ReadSocketService(this.socketChannel);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }


    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }


    /**
     * 处理select读事件
     * 
     * @return 返回处理结果
     */
    public boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    readSizeZeroTimes = 0;
                    this.dispatchReadRequest();
                }
                else if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                }
                else {
                    // TODO ERROR
                    System.out.println("read socket < 0");
                    return false;
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }


    private void dispatchReadRequest() {
        int writePostion = this.byteBufferRead.position();
        // 针对线程池优化
        final List<ByteBuffer> requestList = new LinkedList<ByteBuffer>();

        while (true) {
            int diff = this.byteBufferRead.position() - this.dispatchPostion;
            if (diff >= 8) {
                // msgSize不包含消息reqId
                int msgSize = this.byteBufferRead.getInt(this.dispatchPostion);
                final Integer reqId = this.byteBufferRead.getInt(this.dispatchPostion + 4);
                // 可以凑够一个请求
                if (diff >= (8 + msgSize)) {
                    this.byteBufferRead.position(0);
                    final ByteBuffer request = this.byteBufferRead.slice();
                    request.position(this.dispatchPostion + 8);
                    request.limit(this.dispatchPostion + 8 + msgSize);
                    this.byteBufferRead.position(writePostion);
                    this.dispatchPostion += 8 + msgSize;

                    if (this.executor != null) {
                        // if (this.executor.getActiveCount() >=
                        // (this.executor.getMaximumPoolSize() - 16)) {
                        // requestList.add(request);
                        // continue;
                        // }

                        try {
                            this.executor.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        byte[] response =
                                                Connection.this.rpcServerProcessor.process(reqId, request);
                                        if (response != null) {
                                            Connection.this.linkeByteBufferList.putData(reqId, response);
                                        }
                                    }
                                    catch (Throwable e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                        catch (RejectedExecutionException e) {
                            requestList.add(request);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    else {
                        byte[] response = Connection.this.rpcServerProcessor.process(reqId, request);
                        if (response != null) {
                            Connection.this.linkeByteBufferList.putData(reqId, response);
                        }
                    }

                    continue;
                }
                // 无法凑够一个请求
                else {
                    // ByteBuffer满了，分配新的内存
                    if (!this.byteBufferRead.hasRemaining()) {
                        this.reallocateByteBuffer();
                    }

                    break;
                }
            }
            else if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        // 一个线程内运行多个任务
        for (boolean retry = true; retry;) {
            try {
                if (!requestList.isEmpty()) {
                    this.executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            for (ByteBuffer request : requestList) {
                                try {
                                    final int reqId = request.getInt(request.position() - 4);
                                    byte[] response =
                                            Connection.this.rpcServerProcessor.process(reqId, request);
                                    if (response != null) {
                                        Connection.this.linkeByteBufferList.putData(reqId, response);
                                    }
                                }
                                catch (Throwable e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    });
                }

                retry = false;
            }
            catch (RejectedExecutionException e) {
                try {
                    Thread.sleep(1);
                }
                catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }


    // private void reallocateByteBuffer() {
    // ByteBuffer bb = ByteBuffer.allocate(ReadMaxBufferSize);
    // int remain = this.byteBufferRead.limit() - this.dispatchPostion;
    // bb.put(this.byteBufferRead.array(), this.dispatchPostion, remain);
    // this.dispatchPostion = 0;
    // this.byteBufferRead = bb;
    // }

    private void reallocateByteBuffer() {
        int remain = this.byteBufferRead.limit() - this.dispatchPostion;
        if (remain > 0) {
            byte[] remainData = new byte[remain];
            this.byteBufferRead.position(this.dispatchPostion);
            this.byteBufferRead.get(remainData);
            this.byteBufferRead.put(remainData, 0, remain);
        }

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(ReadMaxBufferSize);
        this.dispatchPostion = 0;
    }


    public void putRequest(final int reqId, final byte[] data) {
        this.linkeByteBufferList.putData(reqId, data);
    }


    public int getWriteByteBufferCnt() {
        return this.linkeByteBufferList.getNodeTotal();
    }


    public SocketChannel getSocketChannel() {
        return socketChannel;
    }


    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
