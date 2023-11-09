package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class AuthenticationPipeline implements RequestPipeline {

    private final AuthConfig authConfig;

    private final AuthenticationEvaluator authenticationEvaluator;

    public AuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        if (authConfig.isAuthenticationEnabled()) {
            Metadata metadata = GrpcConstants.METADATA.get(Context.current());
            AuthenticationContext authenticationContext = AuthenticationFactory.newContext(authConfig, metadata);
            authenticationEvaluator.evaluate(authenticationContext);
            if (authenticationContext instanceof DefaultAuthenticationContext) {
                headers.put(GrpcConstants.AUTHORIZATION_AK, ((DefaultAuthenticationContext) authenticationContext).getUsername());
            }
        }
    }
}
