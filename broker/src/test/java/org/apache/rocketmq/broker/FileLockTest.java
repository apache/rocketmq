package org.apache.rocketmq.broker;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.channels.OverlappingFileLockException;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.Assert;
import org.junit.Test;

public class FileLockTest {

    private String storePathRootDir = ".";

    @Test
    public void test_checkMQAlreadyStart() throws Exception {

        File file = new File(StorePathConfigHelper.getLockFile(storePathRootDir));
        if (file.exists()) {
            file.delete();
        }
        BrokerStartup brokerStartup = new BrokerStartup();
        Method method = brokerStartup.getClass().getDeclaredMethod("checkMQAlreadyStart", String.class);
        method.setAccessible(true);

        method.invoke(brokerStartup, storePathRootDir);
        
        try {
            method.invoke(brokerStartup, storePathRootDir);
        } catch (Exception e) {
            Assert.assertEquals(true, e.getCause() instanceof OverlappingFileLockException);
        }

    }
}
