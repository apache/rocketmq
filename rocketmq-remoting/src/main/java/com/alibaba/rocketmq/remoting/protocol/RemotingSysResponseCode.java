package com.alibaba.rocketmq.remoting.protocol;

public class RemotingSysResponseCode {
    // 成功
    public static final int SUCCESS = 0;
    // 发生了未捕获异常
    public static final int SYSTEM_ERROR = 1;
    // 由于线程池拥堵，系统繁忙
    public static final int SYSTEM_BUSY = 2;
    // 请求代码不支持
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;
    //事务失败，添加db失败
    public static final int TRANSACTION_FAILED = 4;
}
