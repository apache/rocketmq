package org.apache.rocketmq.proxy.grpc.pipeline;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;

public class ContextInitPipeline implements RequestPipeline {
    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        Context ctx = Context.current();
        headers = GrpcConstants.METADATA.get(ctx);
        context.setLocalAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.LOCAL_ADDRESS))
            .setRemoteAddress(getDefaultStringMetadataInfo(headers, GrpcConstants.REMOTE_ADDRESS))
            .setClientID(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_ID))
            .setProtocolType(ChannelProtocolType.GRPC_V2.getName())
            .setLanguage(getDefaultStringMetadataInfo(headers, GrpcConstants.LANGUAGE))
            .setClientVersion(getDefaultStringMetadataInfo(headers, GrpcConstants.CLIENT_VERSION))
            .setAction(getDefaultStringMetadataInfo(headers, GrpcConstants.SIMPLE_RPC_NAME));
        if (ctx.getDeadline() != null) {
            context.setRemainingMs(ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
        }
    }

    protected String getDefaultStringMetadataInfo(Metadata headers, Metadata.Key<String> key) {
        return StringUtils.defaultString(headers.get(key));
    }
}
