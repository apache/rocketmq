/**
 * $Id: Pair.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.common;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class Pair<T1, T2> {
    private T1 object1;
    private T2 object2;


    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }


    public T1 getObject1() {
        return object1;
    }


    public void setObject1(T1 object1) {
        this.object1 = object1;
    }


    public T2 getObject2() {
        return object2;
    }


    public void setObject2(T2 object2) {
        this.object2 = object2;
    }
}
