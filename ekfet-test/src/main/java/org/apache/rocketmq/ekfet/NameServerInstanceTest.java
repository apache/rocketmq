package org.apache.rocketmq.ekfet;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

public class NameServerInstanceTest {
    protected NamesrvController nameSrvController = null;
    final NamesrvConfig namesrvConfig = new NamesrvConfig();
    final NettyServerConfig nettyServerConfig = new NettyServerConfig();

    public static void main(String[] args) {
        NameServerInstanceTest nameServerInstanceTest = new NameServerInstanceTest();
        nameServerInstanceTest.startup();

    }

    private synchronized void startup() {
        try {
            nettyServerConfig.setListenPort(7986);
            nameSrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
            nameSrvController.initialize();
            nameSrvController.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
