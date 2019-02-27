package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class BrokerStatsDataTest {

    @Test
    public void testFromJson() throws Exception {
        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsDay(brokerStatsItem);
        }

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsHour(brokerStatsItem);
        }

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsMinute(brokerStatsItem);
        }

        String json = RemotingSerializable.toJson(brokerStatsData, true);
        BrokerStatsData brokerStatsDataResult = RemotingSerializable.fromJson(json, BrokerStatsData.class);

        assertThat(brokerStatsDataResult.getStatsMinute().getAvgpt()).isCloseTo(brokerStatsData.getStatsMinute().getAvgpt(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsMinute().getTps()).isCloseTo(brokerStatsData.getStatsMinute().getTps(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsMinute().getSum()).isEqualTo(brokerStatsData.getStatsMinute().getSum());

        assertThat(brokerStatsDataResult.getStatsHour().getAvgpt()).isCloseTo(brokerStatsData.getStatsHour().getAvgpt(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsHour().getTps()).isCloseTo(brokerStatsData.getStatsHour().getTps(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsHour().getSum()).isEqualTo(brokerStatsData.getStatsHour().getSum());

        assertThat(brokerStatsDataResult.getStatsDay().getAvgpt()).isCloseTo(brokerStatsData.getStatsDay().getAvgpt(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsDay().getTps()).isCloseTo(brokerStatsData.getStatsDay().getTps(), within(0.0001));
        assertThat(brokerStatsDataResult.getStatsDay().getSum()).isEqualTo(brokerStatsData.getStatsDay().getSum());
    }
}
