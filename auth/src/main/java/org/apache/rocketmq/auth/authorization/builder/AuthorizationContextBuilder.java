package org.apache.rocketmq.auth.authorization.builder;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthorizationContextBuilder {

    List<DefaultAuthorizationContext> build(Metadata metadata, GeneratedMessageV3 message);

    List<DefaultAuthorizationContext> build(RemotingCommand request, String remoteAddr);
}
