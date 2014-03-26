package com.alibaba.rocketmq.common.protocol;

import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 协议辅助类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQProtosHelper {
    /**
     * 将Broker地址注册到Name Server
     */
    public static boolean registerBrokerToNameServer(final String nsaddr, final String brokerAddr,
            final long timeoutMillis) {
        RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

        try {
            RemotingCommand response = RemotingHelper.invokeSync(nsaddr, request, timeoutMillis);
            if (response != null) {
                return ResponseCode.SUCCESS == response.getCode();
            }
        }
        catch (RemotingConnectException e) {
            e.printStackTrace();
        }
        catch (RemotingSendRequestException e) {
            e.printStackTrace();
        }
        catch (RemotingTimeoutException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }
}
