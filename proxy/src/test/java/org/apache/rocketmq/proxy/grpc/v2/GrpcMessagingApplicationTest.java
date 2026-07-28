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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class GrpcMessagingApplicationTest extends InitConfigTest {
    protected static final String REMOTE_ADDR = "192.168.0.1:8080";
    protected static final String LOCAL_ADDR = "127.0.0.1:8080";
    protected static final String CLIENT_ID = "client-id" + UUID.randomUUID();
    protected static final String JAVA = "JAVA";
    @Mock
    StreamObserver<QueryRouteResponse> queryRouteResponseStreamObserver;
    @Mock
    GrpcMessagingActivity grpcMessagingActivity;
    @Mock
    StreamObserver<TelemetryCommand> telemetryResponseStreamObserver;
    GrpcMessagingApplication grpcMessagingApplication;

    private static final String TOPIC = "topic";
    private static Endpoints grpcEndpoints = Endpoints.newBuilder()
        .setScheme(AddressScheme.IPv4)
        .addAddresses(Address.newBuilder().setHost("127.0.0.1").setPort(8080).build())
        .addAddresses(Address.newBuilder().setHost("127.0.0.2").setPort(8080).build())
        .build();

    private static class TelemetryActivityObserver implements ContextStreamObserver<TelemetryCommand> {
        @Override
        public void onNext(ProxyContext ctx, TelemetryCommand value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }

    private static class TelemetryResponseObserver implements StreamObserver<TelemetryCommand> {
        @Override
        public void onNext(TelemetryCommand value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }

    @Before
    public void setUp() throws Throwable {
        super.before();
        RequestPipeline pipeline = (context, headers, request) -> {
        };
        pipeline = pipeline.pipe(new ContextInitPipeline());
        grpcMessagingApplication = new GrpcMessagingApplication(grpcMessagingActivity, pipeline);
    }

    @After
    public void cleanupApplication() throws Exception {
        grpcMessagingApplication.shutdown();
    }

    private void attachMetadata() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.CLIENT_ID, CLIENT_ID);
        metadata.put(GrpcConstants.LANGUAGE, JAVA);
        metadata.put(GrpcConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(GrpcConstants.LOCAL_ADDRESS, LOCAL_ADDR);
        Assert.assertNotNull(Context.current()
            .withValue(GrpcConstants.METADATA, metadata)
            .attach());
    }

    private ThreadPoolExecutor replaceClientManagerExecutor(int threadCount, int queueCapacity) {
        grpcMessagingApplication.clientManagerThreadPoolExecutor.shutdown();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            1,
            TimeUnit.MINUTES,
            queueCapacity == 0 ? new SynchronousQueue<>() : new ArrayBlockingQueue<>(queueCapacity));
        executor.setRejectedExecutionHandler(grpcMessagingApplication.new GrpcTaskRejectedExecutionHandler());
        grpcMessagingApplication.clientManagerThreadPoolExecutor = executor;
        return executor;
    }

    private Semaphore telemetryEventPermits() throws Exception {
        Field poolField = GrpcMessagingApplication.class.getDeclaredField("telemetryPermitPool");
        poolField.setAccessible(true);
        Object permitPool = poolField.get(grpcMessagingApplication);
        Field permitsField = permitPool.getClass().getDeclaredField("permits");
        permitsField.setAccessible(true);
        return (Semaphore) permitsField.get(permitPool);
    }

    private boolean drainScheduled(StreamObserver<TelemetryCommand> requestObserver) throws Exception {
        Field field = requestObserver.getClass().getDeclaredField("drainScheduled");
        field.setAccessible(true);
        return field.getBoolean(requestObserver);
    }

    @Test
    public void testTelemetryPermitPoolPublishedAsAtomicImmutableSnapshot() throws Exception {
        Field poolField = GrpcMessagingApplication.class.getDeclaredField("telemetryPermitPool");
        assertTrue(Modifier.isVolatile(poolField.getModifiers()));
        Field executorField = poolField.getType().getDeclaredField("executor");
        Field permitsField = poolField.getType().getDeclaredField("permits");
        assertEquals(ThreadPoolExecutor.class, executorField.getType());
        assertEquals(Semaphore.class, permitsField.getType());
        assertTrue(Modifier.isFinal(executorField.getModifiers()));
        assertTrue(Modifier.isFinal(permitsField.getModifiers()));
    }

    @Test
    public void testTelemetryDispatchesInboundCallbacksSerially() throws Exception {
        attachMetadata();
        replaceClientManagerExecutor(2, 4);
        TelemetryCommand first = TelemetryCommand.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, "first"))
            .build();
        TelemetryCommand second = TelemetryCommand.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, "second"))
            .build();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondSeen = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        List<String> order = new CopyOnWriteArrayList<>();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    if (value == first) {
                        order.add("first");
                        firstEntered.countDown();
                        try {
                            assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    } else {
                        order.add("second");
                        secondSeen.countDown();
                    }
                }

                @Override
                public void onCompleted() {
                    order.add("completed");
                    completed.countDown();
                }
            });

        StreamObserver<TelemetryCommand> requestObserver =
            grpcMessagingApplication.telemetry(telemetryResponseStreamObserver);
        requestObserver.onNext(first);
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        requestObserver.onNext(second);
        requestObserver.onCompleted();

        assertEquals(1, secondSeen.getCount());
        assertEquals(1, completed.getCount());
        releaseFirst.countDown();
        assertTrue(completed.await(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("first", "second", "completed"), order);
    }

    @Test
    public void testTelemetryQueuesErrorAndHonorsFirstTerminalCallback() throws Exception {
        attachMetadata();
        replaceClientManagerExecutor(2, 4);
        RuntimeException terminalError = new RuntimeException("terminal");
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch errorSeen = new CountDownLatch(1);
        AtomicInteger onNextCount = new AtomicInteger();
        AtomicInteger errorCount = new AtomicInteger();
        AtomicInteger completionCount = new AtomicInteger();
        List<String> order = new CopyOnWriteArrayList<>();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    onNextCount.incrementAndGet();
                    order.add("next");
                    firstEntered.countDown();
                    try {
                        assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    assertEquals(terminalError, t);
                    errorCount.incrementAndGet();
                    order.add("error");
                    errorSeen.countDown();
                }

                @Override
                public void onCompleted() {
                    completionCount.incrementAndGet();
                }
            });

        StreamObserver<TelemetryCommand> requestObserver =
            grpcMessagingApplication.telemetry(telemetryResponseStreamObserver);
        requestObserver.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        requestObserver.onError(terminalError);
        requestObserver.onCompleted();
        requestObserver.onNext(TelemetryCommand.getDefaultInstance());

        assertEquals(1, errorSeen.getCount());
        releaseFirst.countDown();
        assertTrue(errorSeen.await(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("next", "error"), order);
        assertEquals(1, onNextCount.get());
        assertEquals(1, errorCount.get());
        assertEquals(0, completionCount.get());
    }

    @Test
    public void testTelemetrySerializesResponsesAndDropsWritesAfterTerminal() throws Exception {
        AtomicReference<StreamObserver<TelemetryCommand>> activityResponseObserver = new AtomicReference<>();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any())).thenAnswer(invocation -> {
            activityResponseObserver.set(invocation.getArgument(0));
            return new TelemetryActivityObserver();
        });
        AtomicInteger activeWrites = new AtomicInteger();
        AtomicInteger maximumActiveWrites = new AtomicInteger();
        AtomicInteger nextCount = new AtomicInteger();
        AtomicInteger completionCount = new AtomicInteger();
        AtomicInteger errorCount = new AtomicInteger();
        CountDownLatch firstWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstWrite = new CountDownLatch(1);
        StreamObserver<TelemetryCommand> responseObserver = new TelemetryResponseObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                int active = activeWrites.incrementAndGet();
                maximumActiveWrites.accumulateAndGet(active, Math::max);
                try {
                    if (nextCount.incrementAndGet() == 1) {
                        firstWriteEntered.countDown();
                        assertTrue(releaseFirstWrite.await(5, TimeUnit.SECONDS));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    activeWrites.decrementAndGet();
                }
            }

            @Override
            public void onError(Throwable t) {
                errorCount.incrementAndGet();
            }

            @Override
            public void onCompleted() {
                completionCount.incrementAndGet();
            }
        };

        grpcMessagingApplication.telemetry(responseObserver);
        Thread firstWriter = new Thread(
            () -> activityResponseObserver.get().onNext(TelemetryCommand.getDefaultInstance()));
        Thread secondWriter = new Thread(
            () -> activityResponseObserver.get().onNext(TelemetryCommand.getDefaultInstance()));
        firstWriter.start();
        assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));
        secondWriter.start();
        await().atMost(5, TimeUnit.SECONDS).until(() ->
            secondWriter.getState() == Thread.State.BLOCKED || !secondWriter.isAlive());
        assertEquals(1, maximumActiveWrites.get());
        assertEquals(1, nextCount.get());

        releaseFirstWrite.countDown();
        firstWriter.join(TimeUnit.SECONDS.toMillis(5));
        secondWriter.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(firstWriter.isAlive());
        assertFalse(secondWriter.isAlive());
        activityResponseObserver.get().onCompleted();
        try {
            activityResponseObserver.get().onNext(TelemetryCommand.getDefaultInstance());
            Assert.fail("write after terminal should be rejected");
        } catch (IllegalStateException expected) {
            // Expected. The caller uses this signal to clear its stale observer.
        }
        activityResponseObserver.get().onError(new RuntimeException("ignored"));

        assertEquals(1, maximumActiveWrites.get());
        assertEquals(2, nextCount.get());
        assertEquals(1, completionCount.get());
        assertEquals(0, errorCount.get());
    }

    @Test
    public void testTelemetryTerminalResponseClosesGrpcClientChannelOnNextWrite() {
        AtomicInteger delegateWriteCount = new AtomicInteger();
        AtomicInteger completionCount = new AtomicInteger();
        StreamObserver<TelemetryCommand> serializedResponseObserver =
            TelemetryDispatcher.serializeResponse(new TelemetryResponseObserver() {
                @Override
                public void onNext(TelemetryCommand value) {
                    delegateWriteCount.incrementAndGet();
                }

                @Override
                public void onCompleted() {
                    completionCount.incrementAndGet();
                }
            });
        GrpcClientChannel clientChannel = new GrpcClientChannel(
            Mockito.mock(ProxyRelayService.class),
            Mockito.mock(GrpcClientSettingsManager.class),
            Mockito.mock(GrpcChannelManager.class),
            ProxyContext.create().setRemoteAddress(REMOTE_ADDR).setLocalAddress(LOCAL_ADDR),
            CLIENT_ID);
        clientChannel.setClientObserver(serializedResponseObserver);
        assertTrue(clientChannel.isOpen());
        assertTrue(clientChannel.isActive());
        assertTrue(clientChannel.isWritable());

        serializedResponseObserver.onCompleted();
        clientChannel.writeTelemetryCommand(TelemetryCommand.getDefaultInstance());

        assertEquals(0, delegateWriteCount.get());
        assertEquals(1, completionCount.get());
        assertFalse(clientChannel.isOpen());
        assertFalse(clientChannel.isActive());
        assertFalse(clientChannel.isWritable());
    }

    @Test
    public void testTelemetryPipelineFailureTerminatesOnceAndIgnoresLaterCommands() throws Exception {
        grpcMessagingApplication.shutdown();
        RequestPipeline failingPipeline = (context, headers, request) -> {
            throw new RuntimeException("pipeline failed");
        };
        grpcMessagingApplication = new GrpcMessagingApplication(grpcMessagingActivity, failingPipeline);
        AtomicInteger responseCount = new AtomicInteger();
        AtomicInteger completionCount = new AtomicInteger();
        AtomicInteger activityOnNextCount = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<StreamObserver<TelemetryCommand>> activityResponseObserver = new AtomicReference<>();
        StreamObserver<TelemetryCommand> responseObserver = new TelemetryResponseObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onCompleted() {
                completionCount.incrementAndGet();
                completed.countDown();
            }
        };
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any())).thenAnswer(invocation -> {
            activityResponseObserver.set(invocation.getArgument(0));
            return new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    activityOnNextCount.incrementAndGet();
                }

                @Override
                public void onCompleted() {
                    activityResponseObserver.get().onCompleted();
                }
            };
        });

        StreamObserver<TelemetryCommand> requestObserver =
            grpcMessagingApplication.telemetry(responseObserver);
        requestObserver.onNext(TelemetryCommand.getDefaultInstance());
        requestObserver.onNext(TelemetryCommand.getDefaultInstance());

        assertTrue(completed.await(5, TimeUnit.SECONDS));
        assertEquals(1, responseCount.get());
        assertEquals(1, completionCount.get());
        assertEquals(0, activityOnNextCount.get());
    }

    @Test
    public void testTelemetryPermitBudgetIsGlobalAcrossStreams() throws Exception {
        attachMetadata();
        replaceClientManagerExecutor(1, 1);
        CountDownLatch firstStreamEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstStream = new CountDownLatch(1);
        CountDownLatch secondStreamSeen = new CountDownLatch(1);
        AtomicInteger streamIndex = new AtomicInteger();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any())).thenAnswer(invocation -> {
            int index = streamIndex.getAndIncrement();
            StreamObserver<TelemetryCommand> activityResponseObserver = invocation.getArgument(0);
            return new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    if (index == 0) {
                        firstStreamEntered.countDown();
                        try {
                            assertTrue(releaseFirstStream.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    } else {
                        secondStreamSeen.countDown();
                    }
                }

                @Override
                public void onCompleted() {
                    activityResponseObserver.onCompleted();
                }
            };
        });

        BlockingQueue<TelemetryCommand> firstResponses = new LinkedBlockingQueue<>();
        CountDownLatch firstCompleted = new CountDownLatch(1);
        StreamObserver<TelemetryCommand> firstResponseObserver = new TelemetryResponseObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                firstResponses.add(value);
            }

            @Override
            public void onCompleted() {
                firstCompleted.countDown();
            }
        };
        StreamObserver<TelemetryCommand> firstStream =
            grpcMessagingApplication.telemetry(firstResponseObserver);
        StreamObserver<TelemetryCommand> secondStream =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver());
        firstStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstStreamEntered.await(5, TimeUnit.SECONDS));
        secondStream.onNext(TelemetryCommand.getDefaultInstance());
        assertEquals(0, telemetryEventPermits().availablePermits());

        firstStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstResponses.isEmpty());
        assertEquals(1, firstCompleted.getCount());

        releaseFirstStream.countDown();
        TelemetryCommand flowLimited = firstResponses.poll(5, TimeUnit.SECONDS);
        Assert.assertNotNull(flowLimited);
        assertEquals(Code.TOO_MANY_REQUESTS, flowLimited.getStatus().getCode());
        assertTrue(firstCompleted.await(5, TimeUnit.SECONDS));
        assertTrue(secondStreamSeen.await(5, TimeUnit.SECONDS));
        await().atMost(5, TimeUnit.SECONDS)
            .until(() -> telemetryEventPermits().availablePermits() == 2);
    }

    @Test
    public void testTelemetryRejectionDrainsQueuedEventsAndLaterStreamRecovers() throws Exception {
        attachMetadata();
        grpcMessagingApplication.clientManagerThreadPoolExecutor.shutdown();
        AtomicBoolean reject = new AtomicBoolean();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1,
            1,
            1,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(1)) {
            @Override
            public void execute(Runnable command) {
                if (reject.get()) {
                    getRejectedExecutionHandler().rejectedExecution(command, this);
                    return;
                }
                super.execute(command);
            }
        };
        executor.setRejectedExecutionHandler(grpcMessagingApplication.new GrpcTaskRejectedExecutionHandler());
        grpcMessagingApplication.clientManagerThreadPoolExecutor = executor;
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch recovered = new CountDownLatch(1);
        AtomicInteger activityCount = new AtomicInteger();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    if (activityCount.incrementAndGet() == 1) {
                        firstEntered.countDown();
                        try {
                            assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    } else {
                        recovered.countDown();
                    }
                }
            });
        BlockingQueue<TelemetryCommand> rejectedResponses = new LinkedBlockingQueue<>();
        StreamObserver<TelemetryCommand> firstStream =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver() {
                @Override
                public void onNext(TelemetryCommand value) {
                    rejectedResponses.add(value);
                }
            });
        firstStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        firstStream.onNext(TelemetryCommand.getDefaultInstance());
        reject.set(true);
        releaseFirst.countDown();

        TelemetryCommand rejectedResponse = rejectedResponses.poll(5, TimeUnit.SECONDS);
        Assert.assertNotNull(rejectedResponse);
        assertEquals(Code.TOO_MANY_REQUESTS, rejectedResponse.getStatus().getCode());
        await().atMost(5, TimeUnit.SECONDS).until(() -> !drainScheduled(firstStream));
        assertEquals(2, telemetryEventPermits().availablePermits());

        reject.set(false);
        StreamObserver<TelemetryCommand> laterStream =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver());
        laterStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(recovered.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testTelemetryPermitPoolTracksExecutorReplacement() throws Exception {
        attachMetadata();
        replaceClientManagerExecutor(1, 0);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch initialFlowLimited = new CountDownLatch(1);
        AtomicInteger processedCount = new AtomicInteger();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    int count = processedCount.incrementAndGet();
                    if (count == 1 || count == 2) {
                        if (count == 1) {
                            firstEntered.countDown();
                        }
                        try {
                            assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void onCompleted() {
                }
            });
        StreamObserver<TelemetryCommand> initialStream =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver() {
                @Override
                public void onNext(TelemetryCommand value) {
                    if (value.getStatus().getCode() == Code.TOO_MANY_REQUESTS) {
                        initialFlowLimited.countDown();
                    }
                }
            });
        initialStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        initialStream.onNext(TelemetryCommand.getDefaultInstance());
        releaseFirst.countDown();
        assertTrue(initialFlowLimited.await(5, TimeUnit.SECONDS));
        Semaphore initialPermits = telemetryEventPermits();
        await().atMost(5, TimeUnit.SECONDS).until(() -> initialPermits.availablePermits() == 1);

        replaceClientManagerExecutor(1, 2);
        CountDownLatch replacementEntered = new CountDownLatch(1);
        CountDownLatch releaseReplacement = new CountDownLatch(1);
        CountDownLatch replacementCompleted = new CountDownLatch(1);
        BlockingQueue<TelemetryCommand> replacementResponses = new LinkedBlockingQueue<>();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    if (processedCount.incrementAndGet() == 2) {
                        replacementEntered.countDown();
                        try {
                            assertTrue(releaseReplacement.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void onCompleted() {
                    replacementCompleted.countDown();
                }
            });
        StreamObserver<TelemetryCommand> replacementStream =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver() {
                @Override
                public void onNext(TelemetryCommand value) {
                    replacementResponses.add(value);
                }

                @Override
                public void onCompleted() {
                    replacementCompleted.countDown();
                }
            });
        replacementStream.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(replacementEntered.await(5, TimeUnit.SECONDS));
        replacementStream.onNext(TelemetryCommand.getDefaultInstance());
        replacementStream.onNext(TelemetryCommand.getDefaultInstance());
        replacementStream.onNext(TelemetryCommand.getDefaultInstance());
        Semaphore replacementPermits = telemetryEventPermits();
        Assert.assertNotSame(initialPermits, replacementPermits);
        assertEquals(0, replacementPermits.availablePermits());

        releaseReplacement.countDown();
        TelemetryCommand flowLimited = replacementResponses.poll(5, TimeUnit.SECONDS);
        Assert.assertNotNull(flowLimited);
        assertEquals(Code.TOO_MANY_REQUESTS, flowLimited.getStatus().getCode());
        assertTrue(replacementCompleted.await(5, TimeUnit.SECONDS));
        assertEquals(4, processedCount.get());
        await().atMost(5, TimeUnit.SECONDS)
            .until(() -> replacementPermits.availablePermits() == 3);
    }

    @Test
    public void testTelemetryPermitAcquisitionHotPathDoesNotUseApplicationMonitor() throws Exception {
        attachMetadata();
        replaceClientManagerExecutor(1, 0);
        CountDownLatch firstSeen = new CountDownLatch(1);
        CountDownLatch secondSeen = new CountDownLatch(1);
        AtomicInteger activityCount = new AtomicInteger();
        Mockito.when(grpcMessagingActivity.telemetry(Mockito.any()))
            .thenReturn(new TelemetryActivityObserver() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    if (activityCount.incrementAndGet() == 1) {
                        firstSeen.countDown();
                    } else {
                        secondSeen.countDown();
                    }
                }
            });
        StreamObserver<TelemetryCommand> requestObserver =
            grpcMessagingApplication.telemetry(new TelemetryResponseObserver());
        requestObserver.onNext(TelemetryCommand.getDefaultInstance());
        assertTrue(firstSeen.await(5, TimeUnit.SECONDS));
        await().atMost(5, TimeUnit.SECONDS)
            .until(() -> telemetryEventPermits().availablePermits() == 1);

        CountDownLatch monitorHeld = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        Context callerContext = Context.current();
        Thread monitorHolder = new Thread(() -> {
            synchronized (grpcMessagingApplication) {
                monitorHeld.countDown();
                try {
                    assertTrue(releaseMonitor.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        });
        Thread submitter = new Thread(
            () -> callerContext.run(
                () -> requestObserver.onNext(TelemetryCommand.getDefaultInstance())));
        monitorHolder.start();
        assertTrue(monitorHeld.await(5, TimeUnit.SECONDS));
        submitter.start();
        try {
            assertTrue(secondSeen.await(2, TimeUnit.SECONDS));
        } finally {
            releaseMonitor.countDown();
            monitorHolder.join(TimeUnit.SECONDS.toMillis(5));
            submitter.join(TimeUnit.SECONDS.toMillis(5));
        }
        assertFalse(monitorHolder.isAlive());
        assertFalse(submitter.isAlive());
    }

    @Test
    public void testQueryRoute() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.CLIENT_ID, CLIENT_ID);
        metadata.put(GrpcConstants.LANGUAGE, JAVA);
        metadata.put(GrpcConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(GrpcConstants.LOCAL_ADDRESS, LOCAL_ADDR);
        
        Assert.assertNotNull(Context.current()
            .withValue(GrpcConstants.METADATA, metadata)
            .attach());

        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setEndpoints(grpcEndpoints)
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .build();
        Mockito.when(grpcMessagingActivity.queryRoute(Mockito.any(ProxyContext.class), Mockito.eq(request)))
            .thenReturn(future);
        QueryRouteResponse response = QueryRouteResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .addMessageQueues(MessageQueue.getDefaultInstance())
            .build();
        grpcMessagingApplication.queryRoute(request, queryRouteResponseStreamObserver);
        future.complete(response);
        await().untilAsserted(() -> {
            Mockito.verify(queryRouteResponseStreamObserver, Mockito.times(1)).onNext(Mockito.same(response));
        });
    }

    @Test
    public void testQueryRouteWithBadClientID() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.LANGUAGE, JAVA);
        metadata.put(GrpcConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(GrpcConstants.LOCAL_ADDRESS, LOCAL_ADDR);

        Assert.assertNotNull(Context.current()
            .withValue(GrpcConstants.METADATA, metadata)
            .attach());

        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setEndpoints(grpcEndpoints)
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .build();
        grpcMessagingApplication.queryRoute(request, queryRouteResponseStreamObserver);

        ArgumentCaptor<QueryRouteResponse> responseArgumentCaptor = ArgumentCaptor.forClass(QueryRouteResponse.class);
        await().untilAsserted(() -> {
            Mockito.verify(queryRouteResponseStreamObserver, Mockito.times(1)).onNext(responseArgumentCaptor.capture());
        });

        assertEquals(Code.CLIENT_ID_REQUIRED, responseArgumentCaptor.getValue().getStatus().getCode());
    }
}
