/**
 * $Id$
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtosHelper;


/**
 * 管理Producer组及各个Producer连接
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(MixAll.BrokerLoggerName);
    private static final long LockTimeoutMillis = 3000;

    private final Lock hashcodeChannelLock = new ReentrantLock();
    private final HashMap<Integer, List<Channel>> hashcodeChannelTable = new HashMap<Integer, List<Channel>>();

    private final Lock groupChannelLock = new ReentrantLock();
    private final HashMap<String, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();


    public ProducerManager() {

    }


    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.hashcodeChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    List<Channel> chlList = this.hashcodeChannelTable.get(group.hashCode());
                    if (null == chlList) {
                        chlList = new ArrayList<Channel>();
                        this.hashcodeChannelTable.put(group.hashCode(), chlList);
                    }

                    if (!chlList.contains(clientChannelInfo.getChannel())) {
                        chlList.add(clientChannelInfo.getChannel());
                    }
                }
                finally {
                    this.hashcodeChannelLock.unlock();
                }
            }
            else {
                log.warn("registerProducer lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }

        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<Channel, ClientChannelInfo>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    if (!channelTable.containsKey(clientChannelInfo.getChannel())) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
            else {
                log.warn("registerProducer lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }
}
