/**
 * $Id: Client.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc.benchmark;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.alibaba.rocketmq.research.rpc.DefaultRPCClient;
import com.alibaba.rocketmq.research.rpc.RPCClient;


/**
 * 简单功能测试，Client端
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class Client {
    public static void main(String[] args) {
        RPCClient rpcClient = new DefaultRPCClient();
        boolean connectOK = rpcClient.connect(new InetSocketAddress("127.0.0.1", 2012), 1);
        System.out.println("connect server " + (connectOK ? "OK" : "Failed"));
        rpcClient.start();

        for (long i = 0;; i++) {
            try {
                String reqstr = "nice" + i;
                ByteBuffer repdata = rpcClient.call(reqstr.getBytes());
                if (repdata != null) {
                    String repstr =
                            new String(repdata.array(), repdata.position(), repdata.limit()
                                    - repdata.position());
                    System.out.println("call result, " + repstr);
                }
                else {
                    return;
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
