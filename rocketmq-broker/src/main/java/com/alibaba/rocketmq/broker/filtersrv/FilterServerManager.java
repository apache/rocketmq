package com.alibaba.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.BrokerStartup;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    // Filter Server最大空闲时间
    public static final long FilterServerMaxIdleTimeMills = 30000;

    private final ConcurrentHashMap<Channel/* 注册连接 */, FilterServerInfo/* filterServer监听端口 */> filterServerTable =
            new ConcurrentHashMap<Channel, FilterServerInfo>(16);

    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

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


    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void start() {
        // 定时检查Filter Server个数，数量不符合，则创建
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    FilterServerManager.this.createFilterServer();
                }
                catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    private String buildStartCommand() {
        String config = "";
        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s", //
                this.brokerController.getBrokerConfig().getRocketmqHome(),//
                config);
        }
        else {
            return String.format("sh %s/bin/startfsrv.sh %s", //
                this.brokerController.getBrokerConfig().getRocketmqHome(),//
                config);
        }
    }


    public void createFilterServer() {
        int more =
                this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        String cmd = this.buildStartCommand();
        for (int i = 0; i < more; i++) {
            FilterServerUtil.callShell(cmd, log);
        }
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
    public void scanNotActiveChannel() {
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


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(),
                remoteAddr);
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
