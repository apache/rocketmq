/**
 * $Id: TopAddressing.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.namesrv;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.utils.HttpTinyClient;
import com.alibaba.rocketmq.common.utils.HttpTinyClient.HttpResult;


/**
 * 寻址服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<jodie.yqd@gmail.com>
 */
public class TopAddressing {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CommonLoggerName);
    private String nsAddr;
    private String wsAddr;


    public TopAddressing(final String wsAddr) {
        this.wsAddr = wsAddr;
    }


    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }


    public final String fetchNSAddr() {
        return fetchNSAddr(true, 3000);
    }


    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        try {
            HttpResult result = HttpTinyClient.httpGet(this.wsAddr, null, null, "UTF-8", timeoutMills);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                }
                else {
                    log.error("fetch nameserver address is null");
                }
            }
            else {
                log.error("fetch nameserver address failed. statusCode={}", result.code);
            }
        }
        catch (IOException e) {
            if (verbose) {
                log.error("fetchZKAddr exception", e);
            }
        }

        if (verbose) {
            String errorMsg =
                    "connect to " + wsAddr + " failed, maybe the domain name " + MixAll.WS_DOMAIN_NAME
                            + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            log.warn(errorMsg);
        }
        return null;
    }


    public String getNsAddr() {
        return nsAddr;
    }


    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
