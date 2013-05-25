/**
 * $Id: GetMessageStatus.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

/**
 * 访问消息返回的状态码
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum GetMessageStatus {
    // 找到消息
    FOUND,
    // offset正确，但是过滤后没有匹配的消息
    NO_MATCHED_MESSAGE,
    // offset正确，但是物理队列消息正在被删除
    MESSAGE_WAS_REMOVING,
    // offset正确，但是从逻辑队列没有找到，可能正在被删除
    OFFSET_FOUND_NULL,
    // offset错误，严重溢出
    OFFSET_OVERFLOW_BADLY,
    // offset错误，溢出1个
    OFFSET_OVERFLOW_ONE,
    // offset错误，太小了
    OFFSET_TOO_SMALL,
    // 没有对应的逻辑队列
    NO_MATCHED_LOGIC_QUEUE,
    // 队列中一条消息都没有
    NO_MESSAGE_IN_QUEUE,
}
