/**
 * $Id: RPCServer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.research.rpc;

/**
 * 一个简单RPC Server
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface RPCServer {
    public void start();


    public void shutdown();


    public void registerProcessor(final RPCProcessor processor);
}
