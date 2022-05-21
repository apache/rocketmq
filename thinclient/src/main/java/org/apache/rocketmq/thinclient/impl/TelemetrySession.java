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

package org.apache.rocketmq.thinclient.impl;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.thinclient.route.Endpoints;

/**
 * Telemetry session is constructed before first communication between client and remote route endpoints.
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public class TelemetrySession implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetrySession.class);

    private final ClientImpl client;
    private final ClientManager clientManager;
    private final Endpoints endpoints;
    private volatile StreamObserver<TelemetryCommand> requestObserver;

    public static ListenableFuture<TelemetrySession> register(ClientImpl client, ClientManager clientManager,
        Endpoints endpoints) {
        return new TelemetrySession(client, clientManager, endpoints).register();
    }

    private TelemetrySession(ClientImpl client, ClientManager clientManager, Endpoints endpoints) {
        this.client = client;
        this.clientManager = clientManager;
        this.endpoints = endpoints;
    }

    private ListenableFuture<TelemetrySession> register() {
        ListenableFuture<TelemetrySession> future;
        try {
            this.init();
            final ClientSettings clientSettings = client.getClientSettings();
            final Settings settings = clientSettings.toProtobuf();
            final TelemetryCommand settingsCommand = TelemetryCommand.newBuilder().setSettings(settings).build();
            this.telemeter(settingsCommand);
            future = Futures.transform(clientSettings.getArrivedFuture(), input -> this, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            SettableFuture<TelemetrySession> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<TelemetrySession>() {
            @Override
            public void onSuccess(TelemetrySession session) {
                LOGGER.info("Register telemetry session successfully, endpoints={}, clientId={}", endpoints, client.getClientId());
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("Failed to register telemetry session, endpoints={}, clientId={}", endpoints, client.getClientId(), t);
                release();
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    /**
     * Release telemetry session.
     */
    public synchronized void release() {
        try {
            if (null != requestObserver) {
                requestObserver.onCompleted();
            }
        } catch (Throwable ignore) {
            // Ignore exception on purpose.
        }
    }

    /**
     * Initialize telemetry session.
     */
    private synchronized void init() throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException, ClientException {
        this.release();
        final Metadata metadata = client.sign();
        this.requestObserver = clientManager.telemetry(endpoints, metadata, Duration.ofNanos(Long.MAX_VALUE), this);
    }

    private void reinit() {
        try {
            init();
        } catch (Throwable ignore) {
            // Ignore exception on purpose.
        }
    }

    /**
     * Telemeter command to remote.
     *
     * @param command appointed command to telemeter
     */
    public void telemeter(TelemetryCommand command) {
        try {
            requestObserver.onNext(command);
        } catch (RuntimeException e) {
            // Cancel RPC.
            requestObserver.onError(e);
            throw e;
        }
    }

    @Override
    public void onNext(TelemetryCommand command) {
        try {
            switch (command.getCommandCase()) {
                case SETTINGS: {
                    final Settings settings = command.getSettings();
                    LOGGER.info("Receive settings from remote, endpoints={}, clientId={}", endpoints, client.getClientId());
                    client.onSettingsCommand(endpoints, settings);
                    break;
                }
                case RECOVER_ORPHANED_TRANSACTION_COMMAND: {
                    final RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand =
                        command.getRecoverOrphanedTransactionCommand();
                    LOGGER.info("Receive orphaned transaction recovery command from remote, endpoints={}, clientId={}", endpoints, client.getClientId());
                    client.onRecoverOrphanedTransactionCommand(endpoints, recoverOrphanedTransactionCommand);
                    break;
                }
                case VERIFY_MESSAGE_COMMAND: {
                    final VerifyMessageCommand verifyMessageCommand = command.getVerifyMessageCommand();
                    LOGGER.info("Receive message verification command from remote, endpoints={}, clientId={}", client.getClientId());
                    client.onVerifyMessageCommand(endpoints, verifyMessageCommand);
                    break;
                }
                case PRINT_THREAD_STACK_TRACE_COMMAND: {
                    final PrintThreadStackTraceCommand printThreadStackTraceCommand =
                        command.getPrintThreadStackTraceCommand();
                    LOGGER.info("Receive thread stack print command from remote, endpoints={}, clientId={}", endpoints, client.getClientId());
                    client.onPrintThreadStackCommand(endpoints, printThreadStackTraceCommand);
                    break;
                }
                default:
                    LOGGER.warn("Receive unrecognized command from remote, endpoints={}, command={}, clientId={}", endpoints, command, client.getClientId());
            }
        } catch (Throwable t) {
            LOGGER.error("[Bug] unexpected exception raised while receiving command from remote, command={}, clientId={}", command, client.getClientId());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}, endpoints={}",
            client.getClientId(), endpoints, throwable);
        reinit();
    }

    @Override
    public void onCompleted() {
        reinit();
    }
}
