package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class AuthorizationPipeline implements RequestPipeline {

    private final AuthConfig authConfig;
    private final AuthorizationEvaluator authorizationEvaluator;

    public AuthorizationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authorizationEvaluator = AuthorizationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        List<AuthorizationContext> contexts = newContexts(context, headers, request);
        authorizationEvaluator.evaluate(contexts);
    }

    protected List<AuthorizationContext> newContexts(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        return AuthorizationFactory.newContexts(authConfig, headers, request);
    }
}
