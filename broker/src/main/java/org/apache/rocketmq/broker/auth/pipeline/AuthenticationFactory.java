/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package org.apache.rocketmq.broker.auth.pipeline;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

class AuthenticationFactory {

    public static AuthenticationContext newContext(AuthConfig authConfig, ChannelHandlerContext ctx,
            RemotingCommand request) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'newContext'");
    }

    public static AuthenticationEvaluator getEvaluator(AuthConfig authConfig) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEvaluator'");
    }

}
