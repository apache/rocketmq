package org.apache.rocketmq.common;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Test;

import java.util.Properties;

/**
 * @author huhui
 * @create 2019-02-22 20:57
 */
public class ConfigurationTest {
    private static final InternalLogger log = InternalLoggerFactory.getLogger("configurationTest");

    @Test
    public void testConfiguration() {
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        Configuration configuration = new Configuration(log, namesrvConfig);
        configuration.getAllConfigsFormatString();
        configuration.getDataVersionJson();
        Properties properties = new Properties();
        properties.setProperty("test", "mq config");
        configuration.setStorePathFromConfig(namesrvConfig, "kvConfigPath");
        configuration.setStorePath("c://");
        configuration.update(properties);
        configuration.registerConfig(nettyServerConfig);
        Properties allConfigs = configuration.getAllConfigs();
        Properties extProter = new Properties();
        extProter.setProperty("hello", "word");
        configuration.registerConfig(extProter);
    }

}