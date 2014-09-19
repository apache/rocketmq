package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 为减少网络传输数量准备
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SendMessageRequestHeaderV2 implements CommandCustomHeader {
    @CFNotNull
    private String a;// producerGroup;
    @CFNotNull
    private String b;// topic;
    @CFNotNull
    private String c;// defaultTopic;
    @CFNotNull
    private Integer d;// defaultTopicQueueNums;
    @CFNotNull
    private Integer e;// queueId;
    @CFNotNull
    private Integer f;// sysFlag;
    @CFNotNull
    private Long g;// bornTimestamp;
    @CFNotNull
    private Integer h;// flag;
    @CFNullable
    private String i;// properties;
    @CFNullable
    private Integer j;// reconsumeTimes;
    @CFNullable
    private boolean k;// unitMode = false;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public static SendMessageRequestHeader createSendMessageRequestHeaderV1(
            final SendMessageRequestHeaderV2 v2) {
        SendMessageRequestHeader v1 = new SendMessageRequestHeader();
        v1.setProducerGroup(v2.a);
        v1.setTopic(v2.b);
        v1.setDefaultTopic(v2.c);
        v1.setDefaultTopicQueueNums(v2.d);
        v1.setQueueId(v2.e);
        v1.setSysFlag(v2.f);
        v1.setBornTimestamp(v2.g);
        v1.setFlag(v2.h);
        v1.setProperties(v2.i);
        v1.setReconsumeTimes(v2.j);
        v1.setUnitMode(v2.k);
        return v1;
    }


    public static SendMessageRequestHeaderV2 createSendMessageRequestHeaderV2(
            final SendMessageRequestHeader v1) {
        SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
        v2.a = v1.getProducerGroup();
        v2.b = v1.getTopic();
        v2.c = v1.getDefaultTopic();
        v2.d = v1.getDefaultTopicQueueNums();
        v2.e = v1.getQueueId();
        v2.f = v1.getSysFlag();
        v2.g = v1.getBornTimestamp();
        v2.h = v1.getFlag();
        v2.i = v1.getProperties();
        v2.j = v1.getReconsumeTimes();
        v2.k = v1.isUnitMode();
        return v2;
    }


    public String getA() {
        return a;
    }


    public void setA(String a) {
        this.a = a;
    }


    public String getB() {
        return b;
    }


    public void setB(String b) {
        this.b = b;
    }


    public String getC() {
        return c;
    }


    public void setC(String c) {
        this.c = c;
    }


    public Integer getD() {
        return d;
    }


    public void setD(Integer d) {
        this.d = d;
    }


    public Integer getE() {
        return e;
    }


    public void setE(Integer e) {
        this.e = e;
    }


    public Integer getF() {
        return f;
    }


    public void setF(Integer f) {
        this.f = f;
    }


    public Long getG() {
        return g;
    }


    public void setG(Long g) {
        this.g = g;
    }


    public Integer getH() {
        return h;
    }


    public void setH(Integer h) {
        this.h = h;
    }


    public String getI() {
        return i;
    }


    public void setI(String i) {
        this.i = i;
    }


    public Integer getJ() {
        return j;
    }


    public void setJ(Integer j) {
        this.j = j;
    }


    public boolean isK() {
        return k;
    }


    public void setK(boolean k) {
        this.k = k;
    }
}
