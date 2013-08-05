/**
 * $Id: DefaultRPCServer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.common.ServiceThread;


/**
 * 服务端实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultRPCServer implements RPCServer {
    private final int listenPort;
    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    private SocketAddress socketAddressListen;
    private RPCProcessor rpcServerProcessor;

    private List<Connection> connectionList = new LinkedList<Connection>();
    private final AcceptSocketService acceptSocketService = new AcceptSocketService();
    private final ThreadPoolExecutor executor;

    class AcceptSocketService extends ServiceThread {

        @Override
        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    DefaultRPCServer.this.selector.select(1000);
                    Set<SelectionKey> selected = DefaultRPCServer.this.selector.selectedKeys();
                    ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                    Collections.shuffle(selectedList);
                    for (SelectionKey k : selectedList) {
                        if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                            System.out.println("receive new connection, "
                                    + sc.socket().getRemoteSocketAddress());
                            Connection newConnection =
                                    new Connection(sc, DefaultRPCServer.this.rpcServerProcessor,
                                        DefaultRPCServer.this.executor);

                            // if (DefaultRPCServer.this.clientConnection !=
                            // null) {
                            // System.out.println("close old client connection, "
                            // +
                            // DefaultRPCServer.this.clientConnection.getSocketChannel().socket()
                            // .getRemoteSocketAddress());
                            // DefaultRPCServer.this.clientConnection.shutdown();
                            // }

                            DefaultRPCServer.this.connectionList.add(newConnection);
                            newConnection.start();
                        }
                        // TODO， CLOSE SOCKET
                        else {
                            System.out.println("Unexpected ops in select " + k.readyOps());
                        }
                    }

                    selected.clear();
                }
                catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                }
            }

            System.out.println(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }


    public DefaultRPCServer(final int listenPort, final int minPoolSize, final int maxPoolSize)
            throws IOException {
        this.listenPort = listenPort;
        this.socketAddressListen = new InetSocketAddress(this.listenPort);
        this.serverSocketChannel = ServerSocketChannel.open();
        this.selector = Selector.open();
        this.serverSocketChannel.socket().setReuseAddress(true);
        this.serverSocketChannel.socket().bind(this.socketAddressListen);
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);

        this.executor =
                new ThreadPoolExecutor(minPoolSize, maxPoolSize, 60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>(), new ThreadFactory() {
                        private volatile long threadCnt = 0;


                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "RPCHandleThreadPool_" + String.valueOf(this.threadCnt++));
                        }
                    });
    }


    @Override
    public void start() {
        this.acceptSocketService.start();
    }


    @Override
    public void shutdown() {
        this.acceptSocketService.shutdown();

        for (Connection c : this.connectionList) {
            c.shutdown();
        }

        try {
            this.selector.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        try {
            this.serverSocketChannel.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void registerProcessor(RPCProcessor processor) {
        this.rpcServerProcessor = processor;
    }
}
