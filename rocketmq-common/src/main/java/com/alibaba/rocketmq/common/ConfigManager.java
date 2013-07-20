package com.alibaba.rocketmq.common;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * 各种配置的管理接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public abstract class ConfigManager {
    private static final Logger plog = LoggerFactory.getLogger(LoggerName.CommonLoggerName);


    public abstract String encode();


    public abstract String encode(final boolean prettyFormat);


    public abstract void decode(final String jsonString);


    public abstract String configFilePath();


    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                this.decode(jsonString);
                plog.error("load " + fileName + " OK");
                return true;
            }
        }
        catch (Exception e) {
            plog.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }


    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            }
            catch (IOException e) {
                plog.error("persist file Exception, " + fileName, e);
            }
        }
    }
}
