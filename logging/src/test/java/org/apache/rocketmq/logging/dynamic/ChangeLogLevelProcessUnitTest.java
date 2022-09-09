package org.apache.rocketmq.logging.dynamic;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

public class ChangeLogLevelProcessUnitTest {
    Logger logger = LoggerFactory.getLogger(ChangeLogLevelProcessUnitTest.class);
    
    String serverId = "logback-loglevel-test";
    
    @Test
    public void setLogLevelTest() {
        logger.debug("test start");
        logger.debug("type:{}", StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr());
        AbstractProcessUnitImpl process = ProcessUnitFactory.newInstance(serverId).getChangeLogLevelProcess();
        logger.debug("frame:{}", process.logFrameworkType);
        
        process.setDefaultLevel("ERROR");
        String a = null;
        process.setLogLevel(a);
        logger.debug("now is debug");
        process.setLogLevel("INFO");//改成INFO
        
        logger.debug("验证debug打印");
        logger.info("如果上面没有打印，表示升级到INFO了");
        
        process.setLogLevel("DEBUG");
        logger.debug("end is debug");
    }
}
