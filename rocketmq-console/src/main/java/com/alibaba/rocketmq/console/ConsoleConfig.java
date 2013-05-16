/**
 * $Id: ConsoleConfig.java 1839 2013-05-16 02:12:02Z shijia.wxr $
 */
package com.alibaba.rocketmq.console;

import com.alibaba.rocketmq.common.MixAll;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ConsoleConfig {
    private String metaqHome = System.getenv(MixAll.ROCKETMQ_HOME_ENV);
    private String namesrvAddr = null;
    private String webRootPath;
    private int listenPort = 8888;


    public String getMetaqHome() {
        return metaqHome;
    }


    public void setMetaqHome(String metaqHome) {
        this.metaqHome = metaqHome;
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
