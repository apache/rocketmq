package com.alibaba.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final ConcurrentHashMap<Channel/* 注册连接 */, FilterServerInfo/* filterServer监听端口 */> filterServerTable =
            new ConcurrentHashMap<Channel, FilterServerInfo>(16);

    public static final long FilterServerMaxIdleTimeMills = 30000;

    class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;


        public String getFilterServerAddr() {
            return filterServerAddr;
        }


        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }


        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }


        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }


    public FilterServerManager() {
    }


    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        }
        else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            this.filterServerTable.put(channel, filterServerInfo);
            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }


    /**
     * Filter Server 10s向Broker注册一次，Broker如果发现30s没有注册，则删除它
     */
    public void scanExpiredFilterServer() {
        // 单位毫秒
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            if ((System.currentTimeMillis() - timestamp) > FilterServerMaxIdleTimeMills) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }


    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<String>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }
}
