package com.alibaba.rocketmq.remoting;

import static org.junit.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Executors;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-6
 */
public class NettyIdleTest {
    public static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }


    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        config.setServerChannelMaxIdleTimeSeconds(30);
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.registerProcessor(0, new NettyRequestProcessor() {
            private int i = 0;


            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                System.out.println("processRequest=" + request + " " + (i++));
                request.setRemark("hello, I am respponse " + ctx.channel().remoteAddress());
                return request;
            }
        }, Executors.newCachedThreadPool());
        remotingServer.start();
        return remotingServer;
    }


    // @Test
    public void test_idle_event() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingServer server = createRemotingServer();
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 10; i++) {
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
            System.out.println(i + " invoke result = " + response);
            assertTrue(response != null);

            Thread.sleep(1000 * 10);
        }

        Thread.sleep(1000 * 60);

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }

}
