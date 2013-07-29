package com.alibaba.rocketmq.research.fastjson;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public class Test {

    public static class TestTable {
        private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> testTable =
                new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
        private String remark = "abc";


        public ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> getTestTable() {
            return testTable;
        }


        public String getRemark() {
            return remark;
        }


        public void setRemark(String remark) {
            this.remark = remark;
        }
    }


    public static void main(String[] args) {
        TestTable testTable = new TestTable();
        testTable.getTestTable().put("consumer1", new ConcurrentHashMap<String, Long>());
        testTable.getTestTable().put("consumer2", new ConcurrentHashMap<String, Long>());
        testTable.getTestTable().put("consumer3", new ConcurrentHashMap<String, Long>());

        testTable.getTestTable().get("consumer1").put("A", 100L);
        testTable.getTestTable().get("consumer1").put("B", 200L);
        testTable.getTestTable().get("consumer1").put("C", 300L);

        String jsonString = RemotingSerializable.toJson(testTable, true);

        String jsonStringFmt = JSON.toJSONString(testTable, true);

        System.out.println(jsonString);
        System.out.println(jsonStringFmt);
    }
}
