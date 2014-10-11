package com.alibaba.rocketmq.common.protocol;

import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;


public class ResponseCode extends RemotingSysResponseCode {
    // Broker 刷盘超时
    public static final int FLUSH_DISK_TIMEOUT = 10;
    // Broker 同步双写，Slave不可用
    public static final int SLAVE_NOT_AVAILABLE = 11;
    // Broker 同步双写，等待Slave应答超时
    public static final int FLUSH_SLAVE_TIMEOUT = 12;
    // Broker 消息非法
    public static final int MESSAGE_ILLEGAL = 13;
    // Broker, Namesrv 服务不可用，可能是正在关闭或者权限问题
    public static final int SERVICE_NOT_AVAILABLE = 14;
    // Broker, Namesrv 版本号不支持
    public static final int VERSION_NOT_SUPPORTED = 15;
    // Broker, Namesrv 无权限执行此操作，可能是发、收、或者其他操作
    public static final int NO_PERMISSION = 16;
    // Broker, Topic不存在
    public static final int TOPIC_NOT_EXIST = 17;
    // Broker, Topic已经存在，创建Topic
    public static final int TOPIC_EXIST_ALREADY = 18;
    // Broker 拉消息未找到（请求的Offset等于最大Offset，最大Offset无对应消息）
    public static final int PULL_NOT_FOUND = 19;
    // Broker 可能被过滤，或者误通知等
    public static final int PULL_RETRY_IMMEDIATELY = 20;
    // Broker 拉消息请求的Offset不合法，太小或太大
    public static final int PULL_OFFSET_MOVED = 21;
    // Broker 查询消息未找到
    public static final int QUERY_NOT_FOUND = 22;
    // Broker 订阅关系解析失败
    public static final int SUBSCRIPTION_PARSE_FAILED = 23;
    // Broker 订阅关系不存在
    public static final int SUBSCRIPTION_NOT_EXIST = 24;
    // Broker 订阅关系不是最新的
    public static final int SUBSCRIPTION_NOT_LATEST = 25;
    // Broker 订阅组不存在
    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;
    // Producer 事务应该被提交
    public static final int TRANSACTION_SHOULD_COMMIT = 200;
    // Producer 事务应该被回滚
    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;
    // Producer 事务状态未知
    public static final int TRANSACTION_STATE_UNKNOW = 202;
	// Producer ProducerGroup错误
    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
	// 单元化消息，需要设置 buyerId
	public static final int NO_BUYER_ID = 204;

    // 单元化消息，非本单元消息
    public static final int NOT_IN_CURRENT_UNIT = 205;

    // Consumer不在线
    public static final int CONSUMER_NOT_ONLINE = 206;

    // Consumer消费消息超时
    public static final int CONSUME_MSG_TIMEOUT = 207;
}
