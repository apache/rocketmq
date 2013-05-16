package com.alibaba.rocketmq.namesrv.common;

/**
 * @auther lansheng.zj@taobao.com
 */
public class Result {

    public static Result SUCCESS = new Result(true, "SUCCESS");
    private boolean success;
    private String detail;


    public Result(boolean success, String detail) {
        this.success = success;
        this.detail = detail;
    }


    public boolean isSuccess() {
        return success;
    }


    public String getDetail() {
        return detail;
    }


    @Override
    public String toString() {
        return "success=" + success + ",detail=" + detail;
    }
}
