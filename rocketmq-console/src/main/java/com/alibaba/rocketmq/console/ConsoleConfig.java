/**
 * $Id: ConsoleConfig.java 1839 2013-05-16 02:12:02Z shijia.wxr $
 */
package com.alibaba.rocketmq.console;

import com.alibaba.rocketmq.common.MixAll;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ConsoleConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String webRootPath;
    private int listenPort = 8888;


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public int getListenPort() {
        return listenPort;
    }


    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }


    public String getWebRootPath() {
        return webRootPath;
    }


    public void setWebRootPath(String webRootPath) {
        this.webRootPath = webRootPath;
    }
}
