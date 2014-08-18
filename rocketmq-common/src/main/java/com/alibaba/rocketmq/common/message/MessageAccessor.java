package com.alibaba.rocketmq.common.message;

import java.util.Map;


public class MessageAccessor {

    public static void putProperty(final Message msg, final String name, final String value) {
        msg.putProperty(name, value);
    }


    public static void clearProperty(final Message msg, final String name) {
        msg.clearProperty(name);
    }


    public static void setProperties(final Message msg, Map<String, String> properties) {
        msg.setProperties(properties);
    }


    public static void setTransferFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_TRANSFER_FLAG, unit);
    }


    public static String getTransferFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
    }


    public static void setCorrectionFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_CORRECTION_FLAG, unit);
    }


    public static String getCorrectionFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_CORRECTION_FLAG);
    }


    public static void setOriginMessageId(final Message msg, String OriginMessageId) {
        putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, OriginMessageId);
    }


    public static String getOriginMessageId(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
    }


    public static void setMQ2Flag(final Message msg, String flag) {
        putProperty(msg, MessageConst.PROPERTY_MQ2_FLAG, flag);
    }


    public static String getMQ2Flag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MQ2_FLAG);
    }


    public static void setReconsumeTime(final Message msg, String reconsumeTimes) {
        putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, reconsumeTimes);
    }


    public static String getReconsumeTime(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
    }
}
