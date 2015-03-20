package com.alibaba.rocketmq.client.hook;

import com.alibaba.rocketmq.client.exception.MQClientException;


/**
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-4-9
 */
public interface CheckForbiddenHook {
    public String hookName();


    public void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}
