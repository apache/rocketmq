/**
 * $Id: Server.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.research.rpc.DefaultRPCServer;
import com.alibaba.rocketmq.research.rpc.RPCProcessor;
import com.alibaba.rocketmq.research.rpc.RPCServer;


/**
 * 简单功能测试，Server端
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Server {
    static class ServerRPCProcessor implements RPCProcessor {
        private final AtomicLong invokeTimesTotal = new AtomicLong(0);


        @Override
        public byte[] process(int upId, ByteBuffer upstream) {
            // String upstr =
            // new String(upstream.array(), upstream.position(),
            // upstream.limit() - upstream.position());
            // Long value = this.invokeTimesTotal.getAndIncrement();
            // //System.out.println("server process, receive [" + upstr + "], "
            // + value);
            // return value.toString().getBytes();

            int length = upstream.limit() - upstream.position();
            byte[] response = new byte[length];
            upstream.get(response);
            return response;
        }


        public AtomicLong getInvokeTimesTotal() {
            return invokeTimesTotal;
        }
    }


    public static void main(String[] args) {
        try {
            if (args.length > 2) {
                System.err.println("Useage: mtclient [listenPort] [threadCnt]");
                return;
            }

            // args
            int listenPort = args.length > 0 ? Integer.valueOf(args[0]) : 2012;
            int threadCnt = args.length > 1 ? Integer.valueOf(args[1]) : 256;

            RPCServer rpcServer = new DefaultRPCServer(listenPort, threadCnt, threadCnt);
            ServerRPCProcessor serverRPCProcessor = new ServerRPCProcessor();
            rpcServer.registerProcessor(serverRPCProcessor);
            rpcServer.start();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
