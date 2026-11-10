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
package org.apache.rocketmq.auth.repro;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import java.util.List;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider;
import org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.builder.DefaultAuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider;
import org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Characterization test for apache/rocketmq#11178.
 *
 * <p>This test characterizes the behavior of the <em>unguarded lower layers</em> — the
 * Remoting authorization context builder and evaluator — when they are invoked directly.
 * It demonstrates that {@code DefaultAuthorizationContextBuilder} derives the caller identity
 * from the client-supplied {@code AccessKey} with no signature verification, and that the
 * evaluator then authorizes that forged identity.
 *
 * <p>It is <em>not</em> a test of the fix: the fix is the startup guard added in
 * {@code AuthConfig#validate()} (invoked by {@code BrokerController.initialize()} and the proxy
 * {@code AuthorizationPipeline} constructors), which rejects {@code authorizationEnabled=true}
 * with {@code authenticationEnabled=false} before any of this code can be reached in a real
 * deployment.
 */
@RunWith(MockitoJUnitRunner.class)
public class AccessKeySpoofingReproTest {

    private static final String VICTIM_ACCESS_KEY = "rocketmq2";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private AuthConfig authConfig;
    private AuthenticationMetadataManager authenticationMetadataManager;

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Mock
    private Channel channel;

    @Before
    public void setUp() throws Exception {
        RequestHeaderRegistry.getInstance().initialize();

        // Deliberately build the vulnerable combination directly; in a real deployment the
        // startup guard in AuthConfig#validate() rejects it before this code runs.
        this.authConfig = new AuthConfig();
        this.authConfig.setConfigName("repro-" + System.nanoTime());
        this.authConfig.setClusterName("DefaultCluster");
        this.authConfig.setAuthConfigPath(temporaryFolder.newFolder("auth").getAbsolutePath());
        this.authConfig.setAuthenticationEnabled(false);
        this.authConfig.setAuthorizationEnabled(true);
        this.authConfig.setAuthenticationProvider(DefaultAuthenticationProvider.class.getName());
        this.authConfig.setAuthenticationMetadataProvider(LocalAuthenticationMetadataProvider.class.getName());
        this.authConfig.setAuthorizationProvider(DefaultAuthorizationProvider.class.getName());
        this.authConfig.setAuthorizationMetadataProvider(LocalAuthorizationMetadataProvider.class.getName());

        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(this.authConfig);

        // Seed the victim: a SUPER admin user whose AccessKey is "rocketmq2".
        this.authenticationMetadataManager.createUser(
            User.of(VICTIM_ACCESS_KEY, "super-secret-password", UserType.SUPER)).join();
    }

    @After
    public void tearDown() {
        if (this.authenticationMetadataManager != null) {
            this.authenticationMetadataManager.shutdown();
        }
    }

    @Test
    public void forgedSuperAccessKeyIsAuthorized_whenAuthenticationDisabled() {
        // Attacker sends SEND_MESSAGE claiming AccessKey=rocketmq2, WITHOUT any signature.
        RemotingCommand forged = buildSendMessageCommand(VICTIM_ACCESS_KEY);
        mockRemotingChannel();

        // (1) The authorization context builder takes the subject directly from the
        //     client-controlled extFields — no signature / secret key is verified.
        DefaultAuthorizationContextBuilder builder = new DefaultAuthorizationContextBuilder(authConfig);
        List<DefaultAuthorizationContext> contexts = builder.build(channelHandlerContext, forged);
        Assert.assertEquals(1, contexts.size());
        Assert.assertEquals("User:" + VICTIM_ACCESS_KEY, contexts.get(0).getSubject().getSubjectKey());

        // (2) The authorization evaluator ALLOWs the forged identity (SUPER bypasses ACL).
        //     Reaching here without an exception means the forged request was authorized.
        AuthorizationEvaluator evaluator = AuthorizationFactory.getEvaluator(authConfig);
        evaluator.evaluate(forged, contexts);
    }

    @Test
    public void unknownAccessKeyIsDenied() {
        // Contrast: the authorization path DOES enforce identity, but only against the
        // forgeable AccessKey field. An AccessKey with no matching user is denied.
        RemotingCommand forged = buildSendMessageCommand("no_such_user");
        mockRemotingChannel();

        DefaultAuthorizationContextBuilder builder = new DefaultAuthorizationContextBuilder(authConfig);
        List<DefaultAuthorizationContext> contexts = builder.build(channelHandlerContext, forged);
        Assert.assertEquals("User:no_such_user", contexts.get(0).getSubject().getSubjectKey());

        AuthorizationEvaluator evaluator = AuthorizationFactory.getEvaluator(authConfig);
        Assert.assertThrows(AuthorizationException.class, () -> evaluator.evaluate(forged, contexts));
    }

    @Test
    public void forgedAccessKeyRejectedByAuthentication_whenAuthenticationEnabled() {
        // Contrast: with authentication enabled, the same unsigned forged request is rejected
        // at the authentication stage (signature check fails).
        this.authConfig.setAuthenticationEnabled(true);

        RemotingCommand forged = buildSendMessageCommand(VICTIM_ACCESS_KEY);
        mockRemotingChannel();

        DefaultAuthenticationContext authContext =
            new DefaultAuthenticationContextBuilder().build(channelHandlerContext, forged);
        Assert.assertEquals(VICTIM_ACCESS_KEY, authContext.getUsername());

        AuthenticationEvaluator evaluator = AuthenticationFactory.getEvaluator(authConfig);
        Assert.assertThrows(AuthenticationException.class, () -> evaluator.evaluate(authContext));
    }

    private RemotingCommand buildSendMessageCommand(String accessKey) {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setTopic("victim-topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        request.setVersion(441);
        // The attacker simply claims an AccessKey. No Signature / SecretKey is provided.
        request.addExtField("AccessKey", accessKey);
        request.makeCustomHeaderToNet();
        return request;
    }

    private void mockRemotingChannel() {
        ChannelId channelId = mockChannelId("channel-id");
        Attribute<String> proxyAddr = mockAttribute("192.168.0.1");
        Attribute<String> proxyPort = mockAttribute("1234");
        when(channel.id()).thenReturn(channelId);
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(proxyAddr);
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(proxyPort);
        when(channelHandlerContext.channel()).thenReturn(channel);
    }

    private ChannelId mockChannelId(String channelId) {
        ChannelId id = mock(ChannelId.class);
        when(id.asLongText()).thenReturn(channelId);
        return id;
    }

    @SuppressWarnings("unchecked")
    private Attribute<String> mockAttribute(String value) {
        Attribute<String> attribute = mock(Attribute.class);
        when(attribute.get()).thenReturn(value);
        return attribute;
    }
}
