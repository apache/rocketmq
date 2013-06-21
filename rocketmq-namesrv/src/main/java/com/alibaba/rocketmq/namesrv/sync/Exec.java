package com.alibaba.rocketmq.namesrv.sync;

/**
 * 
 * @author lansheng.zj@taobao.com
 * 
 * @param <R>
 *            返回值类型
 * @param <T>
 *            参数类型
 */
public abstract class Exec<R, T> {

    public void beforeExec() {
    }


    public abstract R doExec(T param) throws Exception;


    public void afterExec(R r) {
    }


    public R exec(T param) throws Exception {
        beforeExec();
        R r = doExec(param);
        afterExec(r);
        return r;
    }

}
