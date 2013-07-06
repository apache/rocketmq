/**
 * $Id: NettyRPCTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import static org.junit.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Executors;

import org.junit.Test;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.RequestCode;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 *
 */
public class NettyRPCTest {
    public static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }


    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.registerProcessor(RequestCode.DEMO_REQUEST_VALUE, new NettyRequestProcessor() {
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


    @Test
    public void test_RPC_Sync() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingServer server = createRemotingServer();
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 100; i++) {
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.DEMO_REQUEST_VALUE, null);
            RemotingCommand response = client.invokeSync("127.0.0.1:8888", request, 1000 * 3);
            System.out.println("invoke result = " + response);
            assertTrue(response != null);
        }

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }


    @Test
    public void test_RPC_Oneway() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {
        RemotingServer server = createRemotingServer();
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 100; i++) {
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.DEMO_REQUEST_VALUE, null);
            request.setRemark(String.valueOf(i));
            client.invokeOneway("127.0.0.1:8888", request, 1000 * 3);
        }

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }


    @Test
    public void test_RPC_Async() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {
        RemotingServer server = createRemotingServer();
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 100; i++) {
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.DEMO_REQUEST_VALUE, null);
            request.setRemark(String.valueOf(i));
            client.invokeAsync("127.0.0.1:8888", request, 1000 * 3, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    System.out.println(responseFuture.getResponseCommand());
                }
            });
        }

        Thread.sleep(1000 * 3);

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }


    @Test
    public void test_server_call_client() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        final RemotingServer server = createRemotingServer();
        final RemotingClient client = createRemotingClient();

        server.registerProcessor(RequestCode.DEMO_REQUEST_VALUE, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                try {
                    return server.invokeSync(ctx.channel(), request, 1000 * 10);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (RemotingSendRequestException e) {
                    e.printStackTrace();
                }
                catch (RemotingTimeoutException e) {
                    e.printStackTrace();
                }

                return null;
            }
        }, Executors.newCachedThreadPool());

        client.registerProcessor(RequestCode.DEMO_REQUEST_VALUE, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                System.out.println("client receive server request = " + request);
                request.setRemark("client remark");
                return request;
            }
        }, Executors.newCachedThreadPool());

        for (int i = 0; i < 3; i++) {
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.DEMO_REQUEST_VALUE, null);
            RemotingCommand response = client.invokeSync("127.0.0.1:8888", request, 1000 * 3);
            System.out.println("invoke result = " + response);
            assertTrue(response != null);
        }

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }

}
