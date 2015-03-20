package com.alibaba.rocketmq.client;

import org.junit.Assert;
import org.junit.Test;


public class ValidatorsTest {

    @Test
    public void topicValidatorTest() {
        try {
            Validators.checkTopic("Hello");
            Validators.checkTopic("%RETRY%Hello");
            Validators.checkTopic("_%RETRY%Hello");
            Validators.checkTopic("-%RETRY%Hello");
            Validators.checkTopic("223-%RETRY%Hello");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
}
