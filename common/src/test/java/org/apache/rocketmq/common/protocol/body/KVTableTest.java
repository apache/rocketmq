package org.apache.rocketmq.common.protocol.body;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class KVTableTest {

    @Test
    public void testKVTable(){
        HashMap<String, String> table = new HashMap<>();
        table.put("key1", "value1");
        table.put("key2", "value2");

        KVTable kvTable = new KVTable();
        Assert.assertTrue(kvTable.getTable().size() == 0);

        HashMap<String, String> oriTable = kvTable.getTable();
        kvTable.setTable(table);
        Assert.assertFalse("not ori table ", kvTable.getTable().equals(oriTable));

        Assert.assertTrue(kvTable.getTable().size() == 2);
        Assert.assertTrue(kvTable.getTable().equals(table) );

    }

}
