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

import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * Dispatches callbacks from one telemetry stream in arrival order.
 */
final class TelemetryDispatcher implements StreamObserver<TelemetryCommand>, Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    interface Dependencies {
        ProxyContext createContext();

        void prepareRequest(ProxyContext context, TelemetryCommand request);

        Status flowLimitStatus();

        Status convertExceptionToStatus(Throwable throwable);

        Semaphore acquireEventPermit();

        void execute(Runnable command);

        void writeResponse(StreamObserver<TelemetryCommand> responseObserver, TelemetryCommand response);
    }

    private final StreamObserver<TelemetryCommand> responseObserver;
    private final ContextStreamObserver<TelemetryCommand> activityObserver;
    private final Function<Status, TelemetryCommand> statusResponseCreator;
    private final Dependencies dependencies;
    private final Queue<TelemetryEvent> eventQueue = new ArrayDeque<>();
    private boolean drainScheduled;
    private boolean terminalQueued;

    TelemetryDispatcher(StreamObserver<TelemetryCommand> responseObserver,
        ContextStreamObserver<TelemetryCommand> activityObserver,
        Function<Status, TelemetryCommand> statusResponseCreator, Dependencies dependencies) {
        this.responseObserver = responseObserver;
        this.activityObserver = activityObserver;
        this.statusResponseCreator = statusResponseCreator;
        this.dependencies = dependencies;
    }

    static StreamObserver<TelemetryCommand> serializeResponse(
        StreamObserver<TelemetryCommand> responseObserver) {
        return new SerializedResponseObserver(responseObserver);
    }

    @Override
    public void onNext(TelemetryCommand value) {
        boolean scheduleDrain;
        synchronized (this) {
            if (this.terminalQueued) {
                return;
            }
            ProxyContext context = this.dependencies.createContext();
            TelemetryEvent event;
            boolean terminal = false;
            try {
                this.dependencies.prepareRequest(context, value);
                event = new TelemetryEvent(
                    () -> this.activityObserver.onNext(context, value),
                    () -> this.writeStreamingResponse(
                        this.statusResponseCreator.apply(this.dependencies.flowLimitStatus())));
            } catch (Throwable t) {
                event = this.errorTerminalEvent(t);
                terminal = true;
            }

            if (terminal) {
                scheduleDrain = this.enqueueLocked(event, true);
            } else {
                Semaphore permit = this.dependencies.acquireEventPermit();
                if (permit == null) {
                    event = this.flowLimitTerminalEvent();
                    scheduleDrain = this.enqueueLocked(event, true);
                } else {
                    event.setPermit(permit);
                    scheduleDrain = this.enqueueLocked(event, false);
                }
            }
        }
        if (scheduleDrain) {
            this.scheduleDrain();
        }
    }

    @Override
    public void onError(Throwable t) {
        boolean scheduleDrain;
        synchronized (this) {
            scheduleDrain = this.enqueueLocked(new TelemetryEvent(
                () -> this.activityObserver.onError(t),
                () -> this.activityObserver.onError(t)), true);
        }
        if (scheduleDrain) {
            this.scheduleDrain();
        }
    }

    @Override
    public void onCompleted() {
        boolean scheduleDrain;
        synchronized (this) {
            scheduleDrain = this.enqueueLocked(new TelemetryEvent(
                this.activityObserver::onCompleted,
                this.activityObserver::onCompleted), true);
        }
        if (scheduleDrain) {
            this.scheduleDrain();
        }
    }

    private boolean enqueueLocked(TelemetryEvent event, boolean terminal) {
        if (this.terminalQueued) {
            return false;
        }
        if (terminal) {
            this.terminalQueued = true;
        }
        this.eventQueue.add(event);
        if (this.drainScheduled) {
            return false;
        }
        this.drainScheduled = true;
        return true;
    }

    private TelemetryEvent errorTerminalEvent(Throwable throwable) {
        Runnable writeErrorAndComplete = () -> {
            try {
                this.writeStreamingResponse(
                    this.statusResponseCreator.apply(this.dependencies.convertExceptionToStatus(throwable)));
            } finally {
                this.activityObserver.onCompleted();
            }
        };
        return new TelemetryEvent(writeErrorAndComplete, writeErrorAndComplete);
    }

    private TelemetryEvent flowLimitTerminalEvent() {
        Runnable rejectAndComplete = () -> {
            try {
                this.writeStreamingResponse(
                    this.statusResponseCreator.apply(this.dependencies.flowLimitStatus()));
            } finally {
                this.activityObserver.onCompleted();
            }
        };
        return new TelemetryEvent(rejectAndComplete, rejectAndComplete);
    }

    private void writeStreamingResponse(TelemetryCommand response) {
        this.dependencies.writeResponse(this.responseObserver, response);
    }

    private void scheduleDrain() {
        try {
            this.dependencies.execute(this);
        } catch (Throwable t) {
            log.warn("telemetry event dispatch failed", t);
            this.onRejected();
        }
    }

    @Override
    public void run() {
        TelemetryEvent event;
        synchronized (this) {
            event = this.eventQueue.poll();
            if (event == null) {
                this.drainScheduled = false;
                return;
            }
        }
        event.run();

        boolean scheduleNext;
        synchronized (this) {
            scheduleNext = !this.eventQueue.isEmpty();
            if (!scheduleNext) {
                this.drainScheduled = false;
            }
        }
        if (scheduleNext) {
            this.scheduleDrain();
        }
    }

    void onRejected() {
        while (true) {
            List<TelemetryEvent> rejectedEvents = new ArrayList<>();
            synchronized (this) {
                TelemetryEvent event;
                while ((event = this.eventQueue.poll()) != null) {
                    rejectedEvents.add(event);
                }
                if (rejectedEvents.isEmpty()) {
                    this.drainScheduled = false;
                    return;
                }
            }
            for (TelemetryEvent event : rejectedEvents) {
                event.runRejected();
            }
        }
    }

    private static final class SerializedResponseObserver implements StreamObserver<TelemetryCommand> {
        private final StreamObserver<TelemetryCommand> delegate;
        private boolean terminated;

        private SerializedResponseObserver(StreamObserver<TelemetryCommand> delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void onNext(TelemetryCommand value) {
            if (this.terminated) {
                throw new IllegalStateException("telemetry response observer is already terminated");
            }
            this.delegate.onNext(value);
        }

        @Override
        public synchronized void onError(Throwable t) {
            if (!this.terminated) {
                this.terminated = true;
                this.delegate.onError(t);
            }
        }

        @Override
        public synchronized void onCompleted() {
            if (!this.terminated) {
                this.terminated = true;
                this.delegate.onCompleted();
            }
        }
    }

    private static final class TelemetryEvent {
        private final Runnable action;
        private final Runnable rejectedAction;
        private Semaphore permit;

        private TelemetryEvent(Runnable action, Runnable rejectedAction) {
            this.action = action;
            this.rejectedAction = rejectedAction;
        }

        private void setPermit(Semaphore permit) {
            this.permit = permit;
        }

        private void run() {
            try {
                this.action.run();
            } catch (Throwable t) {
                log.warn("telemetry event execution failed", t);
            } finally {
                this.releasePermit();
            }
        }

        private void runRejected() {
            try {
                this.rejectedAction.run();
            } catch (Throwable t) {
                log.warn("telemetry rejected event handling failed", t);
            } finally {
                this.releasePermit();
            }
        }

        private void releasePermit() {
            if (this.permit != null) {
                this.permit.release();
                this.permit = null;
            }
        }
    }
}
