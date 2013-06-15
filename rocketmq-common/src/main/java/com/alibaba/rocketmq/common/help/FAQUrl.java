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

    // FAQ: 客户端对象参数校验合法性
    public static final String CLIENT_PARAMETER_CHECK_URL = //
            "https://github.com/alibaba/RocketMQ/issues/73";

    //
    // FAQ: 未收录异常处理办法
    //
    public static final String UNEXPECTED_EXCEPTION_URL = //
            "https://github.com/alibaba/RocketMQ/issues/64";

    private static final String TipString = "\nFor more infomation, please visit the url, ";


    public static String suggestTodo(final String url) {
        StringBuilder sb = new StringBuilder();
        sb.append(TipString);
        sb.append(url);
        return sb.toString();
    }


    /**
     * 对于没有未异常原因指定FAQ的情况，追加默认FAQ
     */
    public static String attachDefaultURL(final String errorMessage) {
        if (errorMessage != null) {
            int index = errorMessage.indexOf(TipString);
            if (-1 == index) {
                StringBuilder sb = new StringBuilder();
                sb.append(errorMessage);
                sb.append("\n");
                sb.append("For inquiries, please visit the url, ");
                sb.append(UNEXPECTED_EXCEPTION_URL);
                return sb.toString();
            }
        }

        return errorMessage;
    }
}
