package com.alibaba.rocketmq.namesrv.sync;

import static com.alibaba.rocketmq.common.MixAll.Localhost;
import static com.alibaba.rocketmq.namesrv.common.Result.SUCCESS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.namesrv.common.Result;


/**
 * 
 * @author lansheng.zj@taobao.com
 * 
 * @param <R>
 *            返回值类型
 * 
 */
public class FutureGroup<R> {

    private static final Logger log = LoggerFactory.getLogger(MixAll.NamesrvLoggerName);
    private CountDownLatch latch;
    private Map<String, Future<R>> resultMap;
    private Set<String> keys;


    public FutureGroup(CountDownLatch latch, Set<String> keys) {
        this.latch = latch;
        this.keys = keys;
        resultMap = new HashMap<String, Future<R>>();
    }


    public void addResult(String key, Future<R> r) {
        resultMap.put(key, r);
    }


    public Result await(long timeout) {
        try {
            if (latch.await(timeout, TimeUnit.MILLISECONDS))
                return SUCCESS;
            else
                return new Result(false, detailInfo());
        }
        catch (Exception e) {
            log.error("FutureGroup await error.", e);
            return new Result(false, Localhost + " system error " + e.getMessage());
        }
    }


    public Set<String> getKeys() {
        return keys;
    }


    public void setKeys(Set<String> keys) {
        this.keys = keys;
    }


    public Map<String, Future<R>> getResultMap() {
        return resultMap;
    }


    public String detailInfo() {
        StringBuilder info = new StringBuilder();
        for (String key : keys) {
            info.append(key).append(":").append(resultMap.get(key)).append("\n");
        }

        return info.toString();
    }
}
