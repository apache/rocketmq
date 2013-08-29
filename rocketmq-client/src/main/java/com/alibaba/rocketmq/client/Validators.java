package com.alibaba.rocketmq.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.UtilALl;
import com.alibaba.rocketmq.common.message.Message;


/**
 * 有效性检查公用类。
 * 
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-28
 */
public class Validators {
    public static final String validPatternStr = "^[a-zA-Z0-9_-]+$";


    /**
     * 通过正则表达式进行字符匹配
     * 
     * @param origin
     * @param patternStr
     * @return
     */
    public static boolean regularExpressionMatcher(String origin, String patternStr) {
        if (UtilALl.isBlank(origin)) {
            return false;
        }
        if (UtilALl.isBlank(patternStr)) {
            return true;
        }
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }


    /**
     * 通过正则表达式查找匹配的字符
     * 
     * @param origin
     * @param patternStr
     * @return
     */
    public static String getGroupWithRegularExpression(String origin, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        while (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }


    /**
     * topic 有效性检查
     * 
     * @param topic
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkTopic(String topic) throws MQClientException {
        if (UtilALl.isBlank(topic)) {
            throw new MQClientException("the specified topic is blank", null);
        }
        if (!regularExpressionMatcher(topic, validPatternStr)) {
            throw new MQClientException(String.format(
                "the specified topic[%s] contains illegal characters, allowing only %s", topic,
                validPatternStr), null);
        }
        if (topic.length() > 255) {
            throw new MQClientException("the specified topic is longer than topic max length 255.", null);
        }
    }


    /**
     * group 有效性检查
     * 
     * @param group
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilALl.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }
        if (!regularExpressionMatcher(group, validPatternStr)) {
            throw new MQClientException(String.format(
                "the specified group[%s] contains illegal characters, allowing only %s", group,
                validPatternStr), null);
        }
        if (group.length() > 255) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }
    }


    /**
     * message 有效性检查
     * 
     * @param msg
     * @param defaultMQProducer
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer)
            throws MQClientException {
        if (null == msg) {
            throw new MQClientException("the message is null", null);
        }
        // topic
        Validators.checkTopic(msg.getTopic());
        // body
        if (null == msg.getBody()) {
            throw new MQClientException("the message body is null", null);
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException("the message body length is zero", null);
        }

        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException("the message body size over max value, MAX: "
                    + defaultMQProducer.getMaxMessageSize(), null);
        }
    }


    public static void main(String[] args) {
        String pattern = "^(?![-_])(?!.*[-_]$)[a-zA-Z0-9_-]+$";
        System.out.println(regularExpressionMatcher("aaaa_", pattern));
        System.out.println(regularExpressionMatcher("_aaaa", pattern));
        System.out.println(regularExpressionMatcher("aaaa-", pattern));
        System.out.println(regularExpressionMatcher("-aaaa", pattern));
        System.out.println(regularExpressionMatcher("aaaa-aa_000", pattern));
        System.out.println(regularExpressionMatcher("000", pattern));
    }
}
