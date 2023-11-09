package org.apache.rocketmq.auth.authentication.builder;

import io.grpc.Metadata;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthenticationContextBuilder<AuthenticationContext> {

    AuthenticationContext build(Metadata metadata);

    AuthenticationContext build(RemotingCommand request);
}
