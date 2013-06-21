package com.alibaba.rocketmq.namesrv.sync;

/**
 * @author lansheng.zj@taobao.com
 * 
 * @param <V>
 *            返回值类型
 * @param <T>
 *            参数类型
 */
public class Task<V, T> {

    private Exec<V, T> call;


    public Task(Exec<V, T> call) {
        this.call = call;
    }


    public V exec(T t) throws Exception {
        return call.exec(t);
    }
}
