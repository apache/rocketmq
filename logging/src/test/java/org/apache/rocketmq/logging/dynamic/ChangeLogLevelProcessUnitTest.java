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
        
        logger.debug("Verify debug print");
        logger.info("If there is no print above, it means that it has been upgraded to INFO");
        
        process.setLogLevel("DEBUG");
        logger.debug("end is debug");
    }
}
