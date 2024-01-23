package org.apache.rocketmq.proxy.remoting.protocol.http2proxy;

import io.netty.channel.Channel;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import org.apache.commons.codec.DecoderException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HAProxyMessageForwarderTest {

    private HAProxyMessageForwarder haProxyMessageForwarder;

    @Mock
    private Channel outboundChannel;

    @Before
    public void setUp() throws Exception {
        haProxyMessageForwarder = new HAProxyMessageForwarder(outboundChannel);
    }

    @Test
    public void buildHAProxyTLV() throws DecoderException {
        HAProxyTLV haProxyTLV = haProxyMessageForwarder.buildHAProxyTLV("proxy_protocol_tlv_0xe1", "xxxx");
        assert haProxyTLV != null;
        assert haProxyTLV.typeByteValue() == (byte) 0xe1;
    }
}