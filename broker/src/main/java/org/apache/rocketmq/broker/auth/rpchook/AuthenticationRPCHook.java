package org.apache.rocketmq.broker.auth.rpchook;

import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthenticationRPCHook implements RPCHook {

    private final AuthConfig authConfig;
    private final AuthenticationEvaluator evaluator;

    public AuthenticationRPCHook(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthenticationFactory.getEvaluator(authConfig);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (this.evaluator == null) {
            return;
        }
        if (authConfig.isAuthenticationEnabled()) {
            AuthenticationContext context = AuthenticationFactory.newContext(this.authConfig, request);
            this.evaluator.evaluate(context);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }
}
