package org.apache.rocketmq.common;

public class ClientTracerTimeUtil {
    public static final String MESSAGE_TRACER_TIME_ENABLE = "MESSAGE_TRACER_TIME_ENABLE";

    private static boolean isEnableMessageTracerTime = Boolean.parseBoolean(System.getProperty(MESSAGE_TRACER_TIME_ENABLE, Boolean.FALSE.toString()));

    public static boolean isEnableTracerTime() {
        return isEnableMessageTracerTime;
    }
}
