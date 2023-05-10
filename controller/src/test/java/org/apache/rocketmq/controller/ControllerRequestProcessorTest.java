package org.apache.rocketmq.controller;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ControllerRequestProcessorTest {

    private ControllerRequestProcessor controllerRequestProcessor;

    @Before
    public void init() throws Exception {
        controllerRequestProcessor = new ControllerRequestProcessor(new ControllerManager(new ControllerConfig(), new NettyServerConfig(), new NettyClientConfig()));
    }

    @Test
    public void testProcessRequest_UpdateConfigPath() throws Exception {
        final RemotingCommand updateConfigRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONTROLLER_CONFIG, null);
        Properties properties = new Properties();

        // Update allowed value
        properties.setProperty("notifyBrokerRoleChanged", "true");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = controllerRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        // Update disallowed value
        properties.clear();
        properties.setProperty("configStorePath", "test/path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = controllerRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("Can not update config path");

    }
}
