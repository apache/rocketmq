package org.apache.rocketmq.broker.offset;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>(512);

    private ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }
    @Test
    public void  testCleanConsumerOffsetListSuccess(){
        String groupName = "libreGroup";
        // libreGroup groupName in consumerOffset.json
        String consumerOffsetDataSuccess = "{\"dataVersion\":{\"counter\":0," +
                "\"timestamp\":1569126408715},\"offsetTable\":{\"TopicTest@please_rename_unique_group_name_4\":{0:250,1:250,2:250,3:250}," +
                "\"%RETRY%please_rename_unique_group_name_4@please_rename_unique_group_name_4\":{0:0}," +
                "\"liberTopic@libreGroup\":{0:250,1:250,2:250,3:250}," +
                "\"%RETRY%liberTopic@libreGroup\":{0:0}}}";

        ConsumerOffsetManagerTest obj = RemotingSerializable.fromJson(consumerOffsetDataSuccess,
                ConsumerOffsetManagerTest.class);
        assertThat(obj).isNotNull();
        this.offsetTable = obj.offsetTable;
        getOffsetTable().forEach( (key, value) -> {
            int indexAtGroup = key.lastIndexOf(TOPIC_GROUP_SEPARATOR + groupName);
            if (indexAtGroup != -1) {
                String topic = key.substring( 0, indexAtGroup );
                String cleanTopicAtGroup = topic + TOPIC_GROUP_SEPARATOR + groupName;
                ConcurrentMap<Integer, Long> result = getOffsetTable().remove(cleanTopicAtGroup);
                assertThat(result).isNotNull();
                String resultString = "{TopicTest@please_rename_unique_group_name_4={0=250, 1=250, 2=250, 3=250}, %RETRY%please_rename_unique_group_name_4@please_rename_unique_group_name_4={0=0}}";
                assertThat(getOffsetTable().toString()).as(resultString);
            }
        } );
    }
}