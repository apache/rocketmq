package com.alibaba.rocketmq.common.filter.impl;

/**
 * @auther lansheng.zj@taobao.com
 */
public abstract class Op {

    private String symbol;


    protected Op(String symbol) {
        this.symbol = symbol;
    }


    public String getSymbol() {
        return symbol;
    }


    public String toString() {
        return symbol;
    }
}
