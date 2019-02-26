package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryConsumeQueueResponseBodyTest {



    @Test
    public void test(){
        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();

        SubscriptionData subscriptionData = new SubscriptionData();
        ConsumeQueueData data = new ConsumeQueueData();
        data.setBitMap("defaultBitMap");
        data.setEval(false);
        data.setMsg("this is default msg");
        data.setPhysicOffset(10L);
        data.setPhysicSize(1);
        data.setTagsCode(1L);
        List<ConsumeQueueData> list = new ArrayList<ConsumeQueueData>();
        list.add(data);

        body.setQueueData(list);
        body.setFilterData("default filter data");
        body.setMaxQueueIndex(100L);
        body.setMinQueueIndex(1L);
        body.setSubscriptionData(subscriptionData);

        String json = RemotingSerializable.toJson(body, true);
        QueryConsumeQueueResponseBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeQueueResponseBody.class);
        System.out.println(json);
        ConsumeQueueData jsonData = fromJson.getQueueData().get(0);
        assertThat(jsonData.getMsg()).isEqualTo("this is default msg");
        assertThat(jsonData.getPhysicSize()).isEqualTo(1);
        assertThat(fromJson.getSubscriptionData()).isEqualTo(subscriptionData);

    }
}
