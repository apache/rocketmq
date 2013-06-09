package com.alibaba.rocketmq.namesrv.daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.logger.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;


/**
 * 轮训获取web server上namesrv列表信息
 * 
 * @author lansheng.zj@taobao.com
 */
public class PollingAddress extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private NamesrvConfig namesrvConfig;
    private TopAddressing topAddressing;


    public PollingAddress(NamesrvConfig config) {
        namesrvConfig = config;
        topAddressing = new TopAddressing();
    }


    public String fetchAddr() {
        return topAddressing.fetchNSAddr();
    }


    public void setAddrAndFireChange(String addrs) {
        String old = namesrvConfig.getNamesrvAddr();
        namesrvConfig.setNamesrvAddr(addrs);
        namesrvConfig.firePropertyChange("namesrvAddr", old, addrs);
    }


    @Override
    public void run() {
        // 启动的时候手动配置了，则不需要通过webserver动态的获取namesrv集群地址
        if (null != namesrvConfig.getNamesrvAddr() && !"".equals(namesrvConfig.getNamesrvAddr())) {
            return;
        }

        while (!isStoped()) {
            String addrs = fetchAddr();
            if (null != addrs && !addrs.equals(namesrvConfig.getNamesrvAddr())) {
                setAddrAndFireChange(addrs);

                if (log.isInfoEnabled()) {
                    log.info("poll address from web server, addrs=" + addrs);
                }
            }

            waitForRunning(namesrvConfig.getAddressInterval());
        }
    }


    @Override
    public String getServiceName() {
        return "namesrv-polling-address";
    }

}
