package org.apache.rocketmq.logging.dynamic;

/**
 * log frame type enumeration
 */
public enum LogFrameworkType {
    /**
     * log4j framework
     */
    LOG4J
    /**
     * log4j2 framework
     */
    , LOG4J2
    /**
     * logback framework
     */
    , LOGBACK
    /**
     * Unknown log frame
     */
    , UNKNOWN
}
