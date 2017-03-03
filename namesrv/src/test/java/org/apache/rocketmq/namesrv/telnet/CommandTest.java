package org.apache.rocketmq.namesrv.telnet;

import org.junit.Test;

public class CommandTest {
    private static String addr = "192.168.185.172:9876";

    static {
        TelnetCommandUtil.setNamseSrvAddr(addr);
    }

    //@Test
    public void topicStatusTest() {
        //System.out.println("hello");
        //TelnetCommandUtil.setNamseSrvAddr(addr);
        String topicStr = "topicStatus -t TestTopic";
        String str = TelnetCommandUtil.doCmd(topicStr);
        //System.out.println(str);
    }

    @Test
    public void producerConTest() {
        String producerStr = "producerConnection -t 78zzInfo -g 78zzInfoSendGroup ";
        String str = TelnetCommandUtil.doCmd(producerStr);
        //System.out.println(str);
    }

    //@Test
    public void consumerConTest() {
        String consumerCon = "consumerConnection  -g 78zzArbitrationSubGrouphh";
        String str = TelnetCommandUtil.doCmd(consumerCon);
        //System.out.println(str);
    }

    //@Test
    public void consumerProgressTest() {
        String conStr = "consumerProgress  -g 78zzArbitrationSubGroup";
        String str = TelnetCommandUtil.doCmd(conStr);
        //System.out.println(str);
    }

}
