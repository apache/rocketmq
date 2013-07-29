package com.alibaba.rocketmq.test;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.alibaba.rocketmq.test.producer.ProducerTest;


public class TestAll {

    public static Test suite() {

        TestSuite suite = new TestSuite("TestSuite Test");

        suite.addTestSuite(ProducerTest.class);

        return suite;

    }
}
