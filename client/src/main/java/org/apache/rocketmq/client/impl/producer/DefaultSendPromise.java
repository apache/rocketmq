package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendFuture;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by stone
 */
public class DefaultSendPromise extends CountDownLatch implements SendPromise {

    private final Logger logger = ClientLogger.getLog();

    private Executor executor;
    private Object callbacks;
    private volatile Object result;

    public DefaultSendPromise(Executor executor) {
        super(1);

        if (executor == null) {
            throw new NullPointerException("executor");
        }
        if (executor instanceof ExecutorService) {
            ExecutorService service = (ExecutorService) executor;
            if (service.isShutdown() || service.isTerminated()) {
                throw new IllegalArgumentException("terminated executor service");
            }
        }

        this.executor = executor;
    }

    @Override
    public SendPromise complete(SendResult result) {
        if (result == null) {
            throw new NullPointerException("result");
        }

        if (competeOnce(result)) {
            invokeCallbacks();
        }

        return this;
    }

    public boolean competeOnce(Object result) {
        if (isDone()) {
            return false;
        }

        synchronized (this) {
            if (isDone()) {
                return false;
            }

            this.result = result;
            countDown();
        }

        return true;
    }

    @Override
    public SendPromise report(Throwable cause) {
        if (cause == null) {
            throw new NullPointerException("cause");
        }

        if (competeOnce(cause)) {
            invokeCallbacks();
        }

        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SendFuture addCallback(SendCallback callback) {
        if (callback == null) {
            throw new NullPointerException("listener");
        }

        if (isDone()) {
            invoke(callback);
            return this;
        }

        synchronized (this) {
            if (!isDone()) {
                if (callbacks == null) {
                    callbacks = callback;
                    return this;
                }

                List<SendCallback> callbacks;
                if (this.callbacks instanceof List) {
                    callbacks = (List<SendCallback>) this.callbacks;
                    callbacks.add(callback);
                } else {
                    SendCallback previous = (SendCallback) this.callbacks;
                    callbacks = new ArrayList<>();
                    callbacks.add(previous);

                    this.callbacks = callbacks;
                }

                callbacks.add(callback);
                return this;
            }
        }

        invoke(callback);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SendFuture removeCallback(SendCallback listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }

        if (isDone()) {
            return this;
        }

        synchronized (this) {
            if (callbacks == null) {
                return this;
            }

            if (callbacks == listener) {
                callbacks = null;
                return this;
            }

            List<SendCallback> listeners = (List<SendCallback>) this.callbacks;
            listeners.remove(listener);
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    public void invokeCallbacks() {
        if (callbacks == null) {
            return ;
        }

        if (callbacks instanceof List) {
            List<SendCallback> callbacks = (List<SendCallback>) this.callbacks;
            for (SendCallback callback : callbacks) {
                invoke(callback);
            }
        } else {
            SendCallback callback = (SendCallback) this.callbacks;
            invoke(callback);
        }
    }

    public void invoke(final SendCallback callback) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Object result = DefaultSendPromise.this.result;
                    if (result instanceof Throwable) {
                        callback.onException((Throwable) result);
                    } else {
                        callback.onSuccess((SendResult) result);
                    }
                }
            });
        } catch (Throwable cause) {
            if (logger.isWarnEnabled()) {
                logger.warn("invoke listener({}) error",  callback.getClass().getName(), cause);
            }
        }
    }

    @Override
    public Throwable getCause() {
        if (result instanceof Throwable) {
            return (Throwable) result;
        }
        return null;
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public final boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return result != null;
    }

    public SendFuture waitUntil() {
        if (isDone()) {
            return this;
        }

        while (!isDone()) {
            try {
                await();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        return this;
    }

    public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        if (isDone()) {
            return true;
        }

        if (timeout <= 0) {
            return isDone();
        }

        await(timeout, unit);
        return isDone();
    }

    @Override
    public SendResult get() throws InterruptedException, ExecutionException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        waitUntil();

        Throwable cause = getCause();
        if (cause == null) {
            if (!(result instanceof Throwable)) {
                return (SendResult) result;
            }

            return null;
        }

        throw new ExecutionException(cause);
    }

    @Override
    public SendResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        if (waitFor(timeout, unit)) {
            Throwable cause = getCause();
            if (cause == null) {
                if (!(result instanceof Throwable)) {
                    return (SendResult) result;
                }

                return null;
            }

            throw new ExecutionException(cause);
        }

        throw new TimeoutException();
    }
}
