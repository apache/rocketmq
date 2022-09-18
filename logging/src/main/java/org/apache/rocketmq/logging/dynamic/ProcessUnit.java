package org.apache.rocketmq.logging.dynamic;

import com.alibaba.fastjson.JSONArray;

/**
 * log processing interface
 */
public interface ProcessUnit {
    /**
     * default level
     *
     * @param defaultLevel
     */
    void setDefaultLevel(String defaultLevel);
    
    /**
     * log level
     *
     * @param logLevel
     * @return
     */
    String setLogLevel(String logLevel);
    
    /**
     * class log bean
     *
     * @param data
     * @return
     */
    String setLogLevel(JSONArray data);
}
