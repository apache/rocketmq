/**
 * $Id$
 */
package com.alibaba.rocketmq.broker.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;

import com.alibaba.rocketmq.remoting.ChannelEventListener;


/**
 * 定期检测客户端连接，清除不活动的连接
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class ClientHousekeepingService implements ChannelEventListener {
    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ClientHousekeepingScheduledThread");
            }
        });


    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
        // TODO Auto-generated method stub

    }


    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        // TODO Auto-generated method stub

    }


    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        // TODO Auto-generated method stub

    }
}
