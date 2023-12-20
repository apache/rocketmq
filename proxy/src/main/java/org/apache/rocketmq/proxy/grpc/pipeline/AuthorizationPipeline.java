package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
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
        if (this.authorizationEvaluator == null) {
            return;
        }
        if (authConfig.isAuthenticationEnabled()) {
            List<AuthorizationContext> contexts = AuthorizationFactory.newContexts(authConfig, headers, request);
            if (CollectionUtils.isEmpty(contexts)) {
                return;
            }
            authorizationEvaluator.evaluate(contexts);
        }
    }
}
