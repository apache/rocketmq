/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.namesrv;

import java.io.File;

import com.alibaba.rocketmq.common.MixAll;


/**
 * Name server 的配置类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 */
public class NamesrvConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    // 通用的KV配置持久化地址
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv"
            + File.separator + "kvConfig.json";


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getKvConfigPath() {
        return kvConfigPath;
    }


    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }
}
