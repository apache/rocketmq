package com.alibaba.rocketmq.filtersrv.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.utils.HttpTinyClient;
import com.alibaba.rocketmq.common.utils.HttpTinyClient.HttpResult;


public class HttpFilterClassFetchMethod implements FilterClassFetchMethod {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);
    private final String url;


    public HttpFilterClassFetchMethod(String url) {
        this.url = url;
    }


    @Override
    public String fetch(String topic, String consumerGroup, String className) {
        String thisUrl = String.format("%s/%s.java", this.url, className);

        try {
            HttpResult result = HttpTinyClient.httpGet(thisUrl, null, null, "UTF-8", 5000);
            if (200 == result.code) {
                return result.content;
            }
        }
        catch (Exception e) {
            log.error(
                String.format("call <%s> exception, Topic: %s Group: %s", thisUrl, topic, consumerGroup), e);
        }

        return null;
    }
}
