package org.apache.rocketmq.broker.offset;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private ConsumerOffsetManager consumerOffsetManager;

    private static final String key = "FooBar@FooBarGroup";
    @Before
    public void init(){
        consumerOffsetManager = new ConsumerOffsetManager();
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);
        offsetTable.put(key,new ConcurrentHashMap<Integer, Long>(){{
            put(1,2L);
            put(2,3L);
        }});
        consumerOffsetManager.setOffsetTable(offsetTable);
    }

    @Test
    public void cleanOffsetByTopic_NotExist(){
        consumerOffsetManager.cleanOffsetByTopic("InvalidTopic");
        assertThat(consumerOffsetManager.getOffsetTable().containsKey(key));
    }

    @Test
    public void cleanOffsetByTopic_Exist(){
        consumerOffsetManager.cleanOffsetByTopic("FooBar");
        assertThat(!consumerOffsetManager.getOffsetTable().containsKey(key));
    }
}
