/**
 * $Id: RemotingTooMuchRequestException.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.exception;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class RemotingTooMuchRequestException extends RemotingException {
    private static final long serialVersionUID = 4326919581254519654L;


    public RemotingTooMuchRequestException(String message) {
        super(message);
    }
}
