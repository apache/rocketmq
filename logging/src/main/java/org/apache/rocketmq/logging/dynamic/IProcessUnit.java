package org.apache.rocketmq.logging.dynamic;

import com.alibaba.fastjson.JSONArray;

/**
 * 日志处理接口
 */
public interface IProcessUnit {
    /**
     * 默认等级
     *
     * @param defaultLevel
     */
    void setDefaultLevel(String defaultLevel);
    
    /**
     * 日志等级
     *
     * @param logLevel
     * @return
     */
    String setLogLevel(String logLevel);
    
    /**
     * 类日志Bean
     *
     * @param data
     * @return
     */
    String setLogLevel(JSONArray data);
}
