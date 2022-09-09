package org.apache.rocketmq.logging.dynamic;

/**
 * 日志框架类型枚举
 */
public enum LogFrameworkType {
    /**
     * log4j框架
     */
    LOG4J
    /**
     * log4j2框架
     */
    , LOG4J2
    /**
     * logback框架
     */
    , LOGBACK
    /**
     * 未知日志框架
     */
    , UNKNOWN
}
