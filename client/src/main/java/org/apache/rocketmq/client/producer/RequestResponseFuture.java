package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RequestResponseFuture {
    private final String requestUniqId;
    private long timeoutMillis;
    private final RequestCallback requestCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private AtomicBoolean ececuteCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile Message responseMsg = null;
    private volatile boolean sendReqeustOk = true;
    private volatile Throwable cause = null;
    private final Message requestMsg = null;


    public RequestResponseFuture(String requestUniqId, long timeoutMillis, RequestCallback requestCallback){
        this.requestUniqId = requestUniqId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }

    public void executeRequestCallback(){
        if (requestCallback != null) {
            if (sendReqeustOk && cause == null) {
                requestCallback.onSuccess(responseMsg);
            } else {
                requestCallback.onException(cause);
            }
        }
    }

    public boolean isTimeout(){
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public Message waitResponseMessage(final long timeout) throws InterruptedException {
        this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return this.responseMsg;
    }

    public void putResponseMessage(final Message responseMsg){
        this.responseMsg = responseMsg;
        this.countDownLatch.countDown();
    }

    public String getRequestUniqId() {
        return requestUniqId;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public RequestCallback getRequestCallback() {
        return requestCallback;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public AtomicBoolean getEcecuteCallbackOnlyOnce() {
        return ececuteCallbackOnlyOnce;
    }

    public void setEcecuteCallbackOnlyOnce(AtomicBoolean ececuteCallbackOnlyOnce) {
        this.ececuteCallbackOnlyOnce = ececuteCallbackOnlyOnce;
    }

    public Message getResponseMsg() {
        return responseMsg;
    }

    public void setResponseMsg(Message responseMsg) {
        this.responseMsg = responseMsg;
    }

    public boolean isSendReqeustOk() {
        return sendReqeustOk;
    }

    public void setSendReqeustOk(boolean sendReqeustOk) {
        this.sendReqeustOk = sendReqeustOk;
    }

    public Message getRequestMsg() {
        return requestMsg;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}
