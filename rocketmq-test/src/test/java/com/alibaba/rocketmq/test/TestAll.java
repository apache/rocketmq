package com.alibaba.rocketmq.test;

import com.alibaba.rocketmq.test.producer.ProducerTest;

import junit.framework.Test;
import junit.framework.TestSuite;


public class TestAll {

    public static Test suite() {

        TestSuite suite = new TestSuite("TestSuite Test");

        suite.addTestSuite(ProducerTest.class);

        return suite;

    }
}
