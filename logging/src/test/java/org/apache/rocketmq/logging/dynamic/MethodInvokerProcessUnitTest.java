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
        LoggerBean bean = new LoggerBean("com.sekift.logger.MethodInvokerProcessUnitTest", "INFO");
        data.add(bean);
        
        logger.debug("now is debug");
        
        process.setLogLevel(data);
        
        logger.debug("验证debug打印");
        logger.info("如果上面没有打印，表示升级到INFO了");
        
        data.clear();
        bean = new LoggerBean("com.sekift.logger.MethodInvokerProcessUnitTest", "DEBUG");
        data.add(bean);
        
        process.setLogLevel(data);
        logger.debug("end is debug");
    }
}
