package com.alibaba.rocketmq.common.help;

/**
 * 记录一些问题对应的解决方案，减少答疑工作量
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class FAQUrl {
    // FAQ: Topic不存在如何解决
    public static final String APPLY_TOPIC_URL = //
            "https://github.com/alibaba/RocketMQ/issues/55";

    // FAQ: 同一台机器无法启动多个实例（在多个JVM进程中）
    public static final String CLIENT_INSTACNCE_NAME_DUPLICATE_URL = //
            "https://github.com/alibaba/RocketMQ/issues/56";

    // FAQ: Name Server地址不存在
    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = //
            "https://github.com/alibaba/RocketMQ/issues/57";

    // FAQ: 启动Producer、Consumer失败，Group Name重复
    public static final String GROUP_NAME_DUPLICATE_URL = //
            "https://github.com/alibaba/RocketMQ/issues/63";


    public static String suggestTodo(final String url) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("For more infomation, please acccess this url, ");
        sb.append(url);
        return sb.toString();
    }
}
