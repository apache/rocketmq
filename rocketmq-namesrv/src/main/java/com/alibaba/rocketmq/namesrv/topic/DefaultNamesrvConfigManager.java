package com.alibaba.rocketmq.namesrv.topic;

import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;


/**
 * @auther lansheng.zj@taobao.com
 */
public class DefaultNamesrvConfigManager implements NamesrvConfigManager {
    private NamesrvConfig namesrvConfig;


    public DefaultNamesrvConfigManager(NamesrvConfig nConfig) {
        namesrvConfig = nConfig;
    }

}
