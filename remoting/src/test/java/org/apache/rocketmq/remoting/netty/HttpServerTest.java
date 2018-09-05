package org.apache.rocketmq.remoting.netty;

import java.util.HashMap;

import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import com.alibaba.fastjson.JSON;


public class HttpServerTest {

    @Test
    public void remotingCommandSerialize() {

        RemotingCommand cmd = RemotingCommand.createResponseCommand(1, "hello laohu");

        cmd.setLanguage(LanguageCode.valueOf((byte) 0));
        // int version(~32767)
        cmd.setVersion(3);
        // int opaque
        cmd.setOpaque(123112);

        HashMap<String, String> map = new HashMap<String, String>();
        map.put("lao", "laohu");
        map.put("niao", "cai");
        cmd.setExtFields(map);

        String str = JSON.toJSONString(cmd);

        RemotingCommand rc = RemotingSerializable.decode(str.getBytes(), RemotingCommand.class);

    }

}
