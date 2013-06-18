package com.alibaba.rocketmq.common;

/**
 * 各种配置的管理接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public abstract class ConfigManager {
    public abstract String encode();


    public abstract void decode(final String jsonString);


    public abstract String configFilePath();


    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                this.decode(jsonString);
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }


    public synchronized void persist() {
        String jsonString = this.encode();
        if (jsonString != null) {
            String fileName = this.configFilePath();
            MixAll.string2File(jsonString, fileName);
        }
    }
}
