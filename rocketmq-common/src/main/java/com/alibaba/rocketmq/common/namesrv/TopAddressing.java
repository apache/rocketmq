/**
 * $Id: TopAddressing.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.namesrv;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.help.FAQUrl;


/**
 * 寻址服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class TopAddressing {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CommonLoggerName);
    private HttpClient httpClient = new HttpClient();
    private String nsAddr;


    public TopAddressing() {
        HttpConnectionManagerParams managerParams = httpClient.getHttpConnectionManager().getParams();
        managerParams.setConnectionTimeout(5000);
        managerParams.setSoTimeout(5000);
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
        HttpMethod httpMethod = null;

        try {
            httpMethod = new GetMethod(MixAll.WS_ADDR);
            int httpStatus = this.httpClient.executeMethod(httpMethod);
            if (200 == httpStatus) {
                byte[] responseBody = httpMethod.getResponseBody();
                if (responseBody != null) {
                    String responseStr = new String(responseBody);
                    return clearNewLine(responseStr);
                }
                else {
                    log.error("httpMethod.getResponseBody() return null");
                }
            }
            else {
                log.error("HttpClient.executeMethod return not OK, " + httpStatus);
            }
        }
        catch (HttpException e) {
            log.error("fetchZKAddr exception", e);
        }
        catch (IOException e) {
            log.error("fetchZKAddr exception", e);
        }
        finally {
            if (httpMethod != null) {
                httpMethod.releaseConnection();
            }
        }

        String errorMsg =
                "connect to " + MixAll.WS_ADDR + " failed, maybe the domain name " + MixAll.WS_DOMAIN_NAME
                        + " not bind in /etc/hosts";
        errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

        log.warn(errorMsg);
        System.out.println(errorMsg);
        return null;
    }


    protected void doOnNSAddrChanged(final String newNSAddr) {
    }


    public void tryToAddressing() {
        try {
            String newNSAddr = this.fetchNSAddr();
            if (newNSAddr != null) {
                if (null == this.nsAddr || !newNSAddr.equals(this.nsAddr)) {
                    log.info("nsaddr in top web server changed, " + newNSAddr);
                    this.doOnNSAddrChanged(newNSAddr);
                }
            }
        }
        catch (Exception e) {
            log.error("", e);
        }
    }


    public String getNsAddr() {
        return nsAddr;
    }


    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
