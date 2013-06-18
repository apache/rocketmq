package com.alibaba.rocketmq.research.fastjson;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public class Test {

    public static class TestTable {
        private ConcurrentHashMap<String, Long> testTable = new ConcurrentHashMap<String, Long>();


        public ConcurrentHashMap<String, Long> getTestTable() {
            return testTable;
        }


        public void setTestTable(ConcurrentHashMap<String, Long> testTable) {
            this.testTable = testTable;
        }
    }


    public static void main(String[] args) {
        TestTable testTable = new TestTable();
        testTable.getTestTable().put("consumer1", 100L);
        testTable.getTestTable().put("consumer2", 200L);
        testTable.getTestTable().put("consumer3", 400L);

        String jsonString = RemotingSerializable.toJson(testTable);

        System.out.println(jsonString);
    }
}
