package com.alibaba.rocketmq.common.help;

/**
 * 记录一些问题对应的解决方案，减少答疑工作量
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class FAQUrl {
    // Topic不存在，建议用户申请Topic
    public static final String APPLY_TOPIC_URL = //
            "https://github.com/alibaba/RocketMQ/issues/55";

    // Client无法在本机启动多个JVM实例，建议用户修改成不同的实例名称
    public static final String CLIENT_INSTACNCE_NAME_DUPLICATE_URL = //
            "https://github.com/alibaba/RocketMQ/issues/56";

    // Name Server地址不存在
    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = //
            "https://github.com/alibaba/RocketMQ/issues/57";
}
