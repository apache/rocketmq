package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author: bianlanzhou
 * @date: 2019-02-26 09:37
 */
public class ResetOffsetBodyForCTest {
    @Test
    public void testFromJson() {
        int assertSize = 1 ,index = 0;
        String topic = "topic",brokerName = "broker";
        int queueId = 1;
        long offset = 2l;
        ResetOffsetBodyForC robc = new ResetOffsetBodyForC();
        List<MessageQueueForC> offsetTable = new ArrayList<MessageQueueForC>();
        MessageQueueForC messageQueueForC = new MessageQueueForC(topic,brokerName,queueId,offset);
        offsetTable.add(messageQueueForC);
        robc.setOffsetTable(offsetTable);
        String json = RemotingSerializable.toJson(robc, true);
        ResetOffsetBodyForC fromJson = RemotingSerializable.fromJson(json, ResetOffsetBodyForC.class);
        assertThat(fromJson.getOffsetTable().get(index)).isNotNull();
        assertThat(fromJson.getOffsetTable().size()).isEqualTo(assertSize);
        assertThat(fromJson.getOffsetTable().get(index).getBrokerName()).isEqualTo(brokerName);
        assertThat(fromJson.getOffsetTable().get(index).getTopic()).isEqualTo(topic);
        assertThat(fromJson.getOffsetTable().get(index).getQueueId()).isEqualTo(queueId);
        assertThat(fromJson.getOffsetTable().get(index).getOffset()).isEqualTo(offset);
    }
}