package com.alibaba.research.fastjson;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class TestFastjson {
    @Test
    public void test_atomic() {
        OffsetSerializeWrapper writeObject = new OffsetSerializeWrapper();
        writeObject.getOffsetTable().put(new MessageQueue("TopicA", "Broker1", 1), new AtomicLong(100));
        writeObject.getOffsetTable().put(new MessageQueue("TopicB", "Broker2", 2), new AtomicLong(200));
        writeObject.getOffsetTable().put(new MessageQueue("TopicC", "Broker3", 3), new AtomicLong(300));
        System.out.println(writeObject);
        String json = JSON.toJSONString(writeObject);
        System.out.println(json);

        OffsetSerializeWrapper readObject = JSON.parseObject(json, OffsetSerializeWrapper.class);

        System.out.println(readObject);
    }
}


class OffsetSerializeWrapper {
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public ConcurrentHashMap<MessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }


    @Override
    public String toString() {
        return "OffsetSerializeWrapper [offsetTable=" + offsetTable + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((offsetTable == null) ? 0 : offsetTable.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        OffsetSerializeWrapper other = (OffsetSerializeWrapper) obj;
        if (offsetTable == null) {
            if (other.offsetTable != null)
                return false;
        }
        else if (!offsetTable.equals(other.offsetTable))
            return false;
        return true;
    }
}
