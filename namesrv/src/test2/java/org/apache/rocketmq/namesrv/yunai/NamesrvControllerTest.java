package org.apache.rocketmq.namesrv.yunai;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Test;

/**
 * {@link org.apache.rocketmq.namesrv.NamesrvController}单元测试
 * 皮皮虾
 * Created by yunai on 2017/3/25.
 */
public class NamesrvControllerTest {

    /**
     * namesrv 启动
     */
    @Test
    public void testStart() throws Exception {
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }

}
