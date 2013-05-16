package com.alibaba.rocketmq.namesrv.sync;

/**
 * 任务类型
 * 
 * @author lansheng.zj@taobao.com
 */
public abstract class TaskType {

    /**
     * 注册broker的任务
     */
    public static final int REG_BROKER = 1;

    /**
     * 注册topic的任务
     */
    public static final int REG_TOPIC = 2;

    /**
     * 注销broker的任务
     */
    public static final int UNREG_BROKER = 3;

}
