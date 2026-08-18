/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.controller.auth.pipeline;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ControllerAuthenticationPipelineTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NettyRemotingServer server;
    private NettyRemotingClient unsignedClient;
    private NettyRemotingClient signedClient;
    private AuthenticationMetadataManager authenticationMetadataManager;
    private AuthorizationMetadataManager authorizationMetadataManager;

    @Before
    public void setUp() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("controller-auth-pipeline-" + System.nanoTime());
        authConfig.setClusterName("controller-cluster");
        authConfig.setAuthConfigPath(temporaryFolder.newFolder("auth").getAbsolutePath());
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationMetadataProvider(LocalAuthenticationMetadataProvider.class.getName());
        authConfig.setAuthorizationEnabled(true);
        authConfig.setAuthorizationMetadataProvider(LocalAuthorizationMetadataProvider.class.getName());
        authConfig.setInnerClientAuthenticationCredentials(
            "{\"accessKey\":\"controller\",\"secretKey\":\"controller-secret\"}");

        authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        authorizationMetadataManager = AuthorizationFactory.getMetadataManager(authConfig);
        RequestHeaderRegistry.getInstance().initialize();

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(0);
        server = new NettyRemotingServer(serverConfig);
        RequestPipeline pipeline = (ctx, request) -> {
        };
        pipeline = pipeline.pipe(new AuthorizationPipeline(authConfig))
            .pipe(new AuthenticationPipeline(authConfig));
        server.setRequestPipeline(pipeline);
        server.registerProcessor(RequestCode.GET_CONTROLLER_CONFIG,
            (ctx, request) -> RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null), null);
        server.start();

        unsignedClient = new NettyRemotingClient(new NettyClientConfig());
        unsignedClient.start();
        signedClient = new NettyRemotingClient(new NettyClientConfig());
        signedClient.registerRPCHook(new AclClientRPCHook(new SessionCredentials("controller", "controller-secret")));
        signedClient.start();
    }

    @After
    public void tearDown() {
        if (unsignedClient != null) {
            unsignedClient.shutdown();
        }
        if (signedClient != null) {
            signedClient.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
        if (authenticationMetadataManager != null) {
            authenticationMetadataManager.shutdown();
        }
        if (authorizationMetadataManager != null) {
            authorizationMetadataManager.shutdown();
        }
    }

    @Test
    public void rejectsUnsignedControllerManagementRequest() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONTROLLER_CONFIG, null);

        RemotingCommand response = unsignedClient.invokeSync("127.0.0.1:" + server.localListenPort(), request, 3000);

        Assert.assertEquals(ResponseCode.NO_PERMISSION, response.getCode());
    }

    @Test
    public void allowsSignedInnerControllerRequest() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONTROLLER_CONFIG, null);

        RemotingCommand response = signedClient.invokeSync("127.0.0.1:" + server.localListenPort(), request, 3000);

        Assert.assertEquals(ResponseCode.SUCCESS, response.getCode());
    }
}
