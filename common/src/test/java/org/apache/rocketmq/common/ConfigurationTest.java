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
        String allConfigsFormatString = configuration.getAllConfigsFormatString();
        String dataVersionJson = configuration.getDataVersionJson();
        Properties properties = new Properties();
        properties.setProperty("test","mq config");
        configuration.setStorePathFromConfig(namesrvConfig,"kvConfigPath");
        configuration.setStorePath("c://");
        configuration.update(properties);
        System.out.println("--------------------------------");
        System.out.println("dataVersion:"+dataVersionJson+"");
        System.out.println("--------------------------------");
        System.out.println("all config string:\n"+allConfigsFormatString);
        System.out.println("--------------------------------");
        configuration.registerConfig(nettyServerConfig);
        Properties allConfigs = configuration.getAllConfigs();
        allConfigs.forEach((k,v)->{
            System.out.println(k+"-->"+v);
        });
        System.out.println("--------------------------------");
        Properties extProter = new Properties();
        extProter.setProperty("hello","word");
        configuration.registerConfig(extProter);
        System.out.println("all config string:\n"+configuration.getAllConfigsFormatString());
    }

}