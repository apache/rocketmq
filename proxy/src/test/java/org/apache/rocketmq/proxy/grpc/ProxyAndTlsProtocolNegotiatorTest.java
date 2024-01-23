package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyTLV;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAndTlsProtocolNegotiatorTest {

    private ProxyAndTlsProtocolNegotiator negotiator;

    @Before
    public void setUp() throws Exception {
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setTlsTestModeEnable(true);
        negotiator = new ProxyAndTlsProtocolNegotiator();
    }

    @Test
    public void handleHAProxyTLV() {
        ByteBuf content = Unpooled.buffer();
        content.writeBytes("xxxx".getBytes(StandardCharsets.UTF_8));
        HAProxyTLV haProxyTLV = new HAProxyTLV((byte) 0xE1, content);
        negotiator.handleHAProxyTLV(haProxyTLV, Attributes.newBuilder());
    }
}