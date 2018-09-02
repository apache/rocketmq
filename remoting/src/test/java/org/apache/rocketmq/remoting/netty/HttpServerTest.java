package org.apache.rocketmq.remoting.netty;

import java.util.Base64;
import java.util.HashMap;

import org.apache.rocketmq.remoting.netty.http.HttpServerHandler;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;
import org.omg.Messaging.SyncScopeHelper;

import com.alibaba.fastjson.JSON;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpServerTest {

    public static String WEBROOT = "/root";
    public static int PORT = 8080;

    public void httpServerTest() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast("http-decoder", new HttpRequestDecoder());
                        socketChannel.pipeline().addLast("http-aggregator", new HttpObjectAggregator(65536));
                        socketChannel.pipeline().addLast("http-encoder", new HttpResponseEncoder());
                        socketChannel.pipeline().addLast("http-chunked", new ChunkedWriteHandler());
                        socketChannel.pipeline().addLast("fileServerHandler", new HttpServerHandler(null));

                    }
                });
            ChannelFuture future = bootstrap.bind("127.0.0.1", PORT).sync();
            System.out.println("服务器已启动>>网址:" + "127.0.0.1:" + PORT + WEBROOT);
            future.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    @Test
    public void remotingCommandSerialize() {

        RemotingCommand cmd = RemotingCommand.createResponseCommand(1, "hello laohu");

        cmd.setLanguage(LanguageCode.valueOf((byte) 0));
        // int version(~32767)
        cmd.setVersion(3);
        // int opaque
        cmd.setOpaque(123112);
        cmd.setBody("body".getBytes());
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("lao", "laohu");
        map.put("niao", "cai");
        cmd.setExtFields(map);

        String str = JSON.toJSONString(cmd);
        byte[] baseByte = Base64.getDecoder().decode("b");

        System.out.println(Base64.getEncoder().encodeToString(baseByte));

        System.out.println(str);

        RemotingCommand rc = RemotingSerializable.decode(str.getBytes(), RemotingCommand.class);
        System.out.println(rc.toString());

    }

}
