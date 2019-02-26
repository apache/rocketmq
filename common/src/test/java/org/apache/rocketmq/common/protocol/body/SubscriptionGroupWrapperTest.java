package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;
import java.util.concurrent.ConcurrentHashMap;
import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionGroupWrapperTest {

    @Test
    public void testFromJson(){
        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptions = new ConcurrentHashMap<String, SubscriptionGroupConfig>();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setBrokerId(1234);
        subscriptionGroupConfig.setGroupName("Consumer-group-one");
        subscriptions.put("Consumer-group-one", subscriptionGroupConfig);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptions);
        DataVersion dataVersion = new DataVersion();
        dataVersion.nextVersion();
        subscriptionGroupWrapper.setDataVersion(dataVersion);
        String json = RemotingSerializable.toJson(subscriptionGroupWrapper, true);
        SubscriptionGroupWrapper fromJson = RemotingSerializable.fromJson(json, SubscriptionGroupWrapper.class);
        assertThat(fromJson.getSubscriptionGroupTable()).containsKey("Consumer-group-one");
        assertThat(fromJson.getSubscriptionGroupTable().get("Consumer-group-one").getGroupName()).isEqualTo("Consumer-group-one");
        assertThat(fromJson.getSubscriptionGroupTable().get("Consumer-group-one").getBrokerId()).isEqualTo(1234);
    }

}
