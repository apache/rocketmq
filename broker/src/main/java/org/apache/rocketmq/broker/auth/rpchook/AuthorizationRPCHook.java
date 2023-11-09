package org.apache.rocketmq.broker.auth.rpchook;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AuthorizationRPCHook implements RPCHook {

    private final AuthConfig authConfig;

    private final AuthorizationEvaluator evaluator;

    public AuthorizationRPCHook(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.evaluator = AuthorizationFactory.getEvaluator(authConfig);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (this.evaluator == null) {
            return;
        }
        if (authConfig.isAuthorizationEnabled()) {
            List<AuthorizationContext> contexts = AuthorizationFactory.newContexts(this.authConfig, request, remoteAddr);
            if (CollectionUtils.isEmpty(contexts)) {
                return;
            }
            contexts.forEach(this.evaluator::evaluate);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }
}
