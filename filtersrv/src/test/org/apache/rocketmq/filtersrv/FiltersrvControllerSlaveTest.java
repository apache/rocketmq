package org.apache.rocketmq.filtersrv;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Created by yunai on 2017/5/16.
 */
public class FiltersrvControllerSlaveTest {

    public static void main(String[] args) throws Exception {
        // 设置版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(30912);
        //
        final FiltersrvConfig filtersrvConfig = new FiltersrvConfig();
        filtersrvConfig.setNamesrvAddr("127.0.0.1:9876");
        filtersrvConfig.setConnectWhichBroker("127.0.0.1:20911");

        // filter 启动
        FiltersrvController filtersrvController = new FiltersrvController(
                filtersrvConfig,
                nettyServerConfig
        );
        boolean initResult = filtersrvController.initialize();
        System.out.println("initResult：" + initResult);
//        filtersrvController
        filtersrvController.start();
        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }

}
