package org.apache.rocketmq.logging.dynamic;

import com.alibaba.fastjson.JSONArray;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

public class MethodInvokerProcessUnitTest {
    Logger logger = LoggerFactory.getLogger(MethodInvokerProcessUnitTest.class);
    
    String serverId = "logback-method-test";
    
    @Test
    public void setLogLevelTest() {
        logger.debug("test");
        logger.debug("type:{}", StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr());
        AbstractProcessUnitImpl process = ProcessUnitFactory.newInstance(serverId).getMethodInvokerProcess();
        logger.debug("frame:{}", process.logFrameworkType);
        
        JSONArray data = new JSONArray();
        LoggerBean bean = new LoggerBean("org.apache.rocketmq.logging.dynamic.MethodInvokerProcessUnitTest", "INFO");
        data.add(bean);
        
        logger.debug("now is debug");
        
        process.setLogLevel(data);
        
        logger.debug("Verify debug print");
        logger.info("If there is no print above, it means that it has been upgraded to INFO");
        
        data.clear();
        bean = new LoggerBean("org.apache.rocketmq.logging.dynamic.MethodInvokerProcessUnitTest", "DEBUG");
        data.add(bean);
        
        process.setLogLevel(data);
        logger.debug("end is debug");
    }
}
