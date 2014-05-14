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

        private byte[] body = "hello".getBytes();


        public ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> getTestTable() {
            return testTable;
        }


        public String getRemark() {
            return remark;
        }


        public void setRemark(String remark) {
            this.remark = remark;
        }


        public byte[] getBody() {
            return body;
        }


        public void setBody(byte[] body) {
            this.body = body;
        }
    }


    public static void main(String[] args) {
        TestTable testTable = new TestTable();

        testTable.setBody("Hi, I am byte[]".getBytes());

        String jsonString = RemotingSerializable.toJson(testTable, true);

        System.out.println(jsonString);

        TestTable testTable2 = RemotingSerializable.fromJson(jsonString, TestTable.class);

        jsonString = RemotingSerializable.toJson(testTable2, true);

        System.out.println(jsonString);

    }
}
