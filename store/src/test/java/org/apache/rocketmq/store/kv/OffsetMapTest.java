package org.apache.rocketmq.store.kv;

import org.apache.rocketmq.store.kv.CompactionLog.OffsetMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class OffsetMapTest {

    @Test
    public void testPutAndGet() throws Exception {
        OffsetMap offsetMap = new OffsetMap(0);     //min 100 entry
        offsetMap.put("abcde", 1);
        offsetMap.put("abc", 3);
        offsetMap.put("cde", 4);
        offsetMap.put("abcde", 9);
        assertEquals(offsetMap.get("abcde"), 9);
        assertEquals(offsetMap.get("cde"), 4);
        assertEquals(offsetMap.getLastOffset(), 9);
    }

    @Test
    public void testFull() throws Exception {
        OffsetMap offsetMap = new OffsetMap(0);     //min 100 entry
        for (int i = 0; i < 100; i++) {
            offsetMap.put(String.valueOf(i), i);
        }

        assertEquals(offsetMap.get("66"), 66);
        assertNotEquals(offsetMap.get("55"), 56);
        assertEquals(offsetMap.getLastOffset(), 99);
        assertThrows(IllegalArgumentException.class, ()-> offsetMap.put(String.valueOf(100), 100));
    }
}