/**
 * $Id$
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.ChannelEventListener;


/**
 * 定期检测客户端连接，清除不活动的连接
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ClientHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(MixAll.BrokerLoggerName);
    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ClientHousekeepingScheduledThread");
            }
        });


    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void start() {
        // 定时刷消费进度
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ClientHousekeepingService.this.scanExceptionChannel();
                }
                catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }


    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }


    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
    }


    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
    }
}
