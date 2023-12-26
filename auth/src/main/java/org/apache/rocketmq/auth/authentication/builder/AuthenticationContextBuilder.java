package org.apache.rocketmq.auth.authentication.builder;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthenticationContextBuilder<AuthenticationContext> {

    AuthenticationContext build(Metadata metadata, GeneratedMessageV3 request);

    AuthenticationContext build(RemotingCommand request);
}
