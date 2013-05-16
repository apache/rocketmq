/**
 * $Id: HeartbeatData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.rocketmq.common.protocol.MQProtos.ConsumerInfo;
import com.alibaba.rocketmq.common.protocol.MQProtos.HeartbeatInfo;
import com.alibaba.rocketmq.common.protocol.MQProtos.ProducerInfo;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class HeartbeatData {
    private String clientID;
    private Set<ProducerData> producerDataSet = new HashSet<ProducerData>();
    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();


    public byte[] encode() {
        HeartbeatInfo.Builder builder = HeartbeatInfo.newBuilder();

        // clientID
        builder.setClientID(this.clientID);

        // producerDataSet
        if (this.producerDataSet != null) {
            int i = 0;
            for (ProducerData data : this.producerDataSet) {
                builder.addProducerInfos(i++, data.encode());
            }
        }

        // consumerDataSet
        if (this.consumerDataSet != null) {
            int i = 0;
            for (ConsumerData data : this.consumerDataSet) {
                builder.addConsumerInfos(i++, data.encode());
            }
        }

        return builder.build().toByteArray();
    }


    public static HeartbeatData decode(byte[] data) throws InvalidProtocolBufferException {
        if (data != null) {
            HeartbeatData heartbeatData = new HeartbeatData();
            HeartbeatInfo pbClientHeartbeat = HeartbeatInfo.parseFrom(data);

            // clientID
            heartbeatData.setClientID(pbClientHeartbeat.getClientID());

            // producerDataSet
            {
                for (ProducerInfo info : pbClientHeartbeat.getProducerInfosList()) {
                    heartbeatData.getProducerDataSet().add(ProducerData.decode(info));
                }
            }

            // ConsumerData
            {
                for (ConsumerInfo info : pbClientHeartbeat.getConsumerInfosList()) {
                    heartbeatData.getConsumerDataSet().add(ConsumerData.decode(info));
                }
            }

            return heartbeatData;
        }

        return null;
    }


    public String getClientID() {
        return clientID;
    }


    public void setClientID(String clientID) {
        this.clientID = clientID;
    }


    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }


    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }


    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }


    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }


    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
                + ", consumerDataSet=" + consumerDataSet + "]";
    }
}
