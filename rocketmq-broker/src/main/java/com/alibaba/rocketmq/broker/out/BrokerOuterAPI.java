package com.alibaba.rocketmq.broker.out;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-3
 */
public class BrokerOuterAPI {
    private final static Logger log = ClientLogger.getLog();
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing();
    private String nameSrvAddr = null;


    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }


    public void start() {
        this.remotingClient.start();
    }


    public void shutdown() {
        this.remotingClient.shutdown();
    }


    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: " + this.nameSrvAddr + " new: " + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        }
        catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }


    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        if (addrArray != null) {
            for (String addr : addrArray) {
                lst.add(addr);
            }

            this.remotingClient.updateNameServerAddressList(lst);
        }
    }


    public void registerBroker() {
    }


    public void unregisterBroker() {
    }
}
