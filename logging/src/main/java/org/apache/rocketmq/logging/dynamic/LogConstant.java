package org.apache.rocketmq.logging.dynamic;

/**
 * 日志常数
 */
public class LogConstant {
    public static final String LOG4J_LOGGER_FACTORY = "org.slf4j.impl.Log4jLoggerFactory";
    public static final String LOG4J2_LOGGER_FACTORY = "org.apache.logging.slf4j.Log4jLoggerFactory";
    public static final String LOGBACK_LOGGER_FACTORY = "ch.qos.logback.classic.util.ContextSelectorStaticBinder";
    
    public static final String ROOT_KEY = "root";
    public static final String LOGGER_NAME = "loggerName";
    public static final String LOGGER_LEVEL = "loggerLevel";
    public static final String LOGGER_TYPE_UNKNOWN = "Logger的类型未知,无法处理!";
    public static final String LOGGER_NOT_EXSIT = "需要修改日志级别的Logger不存在!";
    public static final String PARAMETER_TYPE_ERROR = "参数类型错误,无法处理!";
    public static final String LOGGER_LIST_IS_NULL = "loggerList为空,无法处理!";
    
    public static final String LOG_FRAMEWORK = "logFramework";
    public static final String LOGGER_LIST = "loggerList";
    
}
