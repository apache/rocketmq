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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimerRequest {

    private final long offsetPy;
    private final int sizePy;
    private final long delayTime;

    private final int magic;

    private long enqueueTime;
    private MessageExt msg;

    /**
     * Completion state of the current attempt. A request may be put more than once when a batch
     * has a partial failure, and every attempt must own its completion state: a late release from
     * the previous attempt must not complete the attempt that replaced it.
     */
    private volatile Attempt attempt;

    /**
     * The attempt handed to the dequeue pipeline. It is captured when the request is handed over
     * so that the pipeline owner releases the attempt it actually owns, even if the enqueue loop
     * has already re-armed the request for a retry.
     */
    private volatile Attempt pipelineAttempt;

    private Set<String> deleteList;

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic) {
        this(offsetPy, sizePy, delayTime, enqueueTime, magic, null);
    }

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic, MessageExt msg) {
        this.offsetPy = offsetPy;
        this.sizePy = sizePy;
        this.delayTime = delayTime;
        this.enqueueTime = enqueueTime;
        this.magic = magic;
        this.msg = msg;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public int getSizePy() {
        return sizePy;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public void setMsg(MessageExt msg) {
        this.msg = msg;
    }

    public int getMagic() {
        return magic;
    }

    public Set<String> getDeleteList() {
        return deleteList;
    }

    public void setDeleteList(Set<String> deleteList) {
        this.deleteList = deleteList;
    }

    /**
     * Start a new attempt and make it the current one. The previous attempt, if any, keeps its own
     * completion state and is released by the owner that was handed the request.
     */
    public Attempt beginAttempt(CountDownLatch latch) {
        Attempt newAttempt = new Attempt(latch);
        this.attempt = newAttempt;
        return newAttempt;
    }

    public Attempt getAttempt() {
        return attempt;
    }

    public Attempt getPipelineAttempt() {
        return pipelineAttempt;
    }

    public void setPipelineAttempt(Attempt pipelineAttempt) {
        this.pipelineAttempt = pipelineAttempt;
    }

    public void setLatch(CountDownLatch latch) {
        beginAttempt(latch);
    }

    public void setEnqueueTime(long enqueueTime) {
        this.enqueueTime = enqueueTime;
    }

    public void idempotentRelease() {
        idempotentRelease(true);
    }

    public void idempotentRelease(boolean succ) {
        Attempt releasedAttempt = attempt;
        if (releasedAttempt != null) {
            releasedAttempt.release(succ);
        }
    }

    public void idempotentRelease(Attempt releasedAttempt, boolean succ) {
        if (releasedAttempt != null) {
            releasedAttempt.release(succ);
        }
    }

    public boolean isSucc() {
        Attempt currentAttempt = attempt;
        return currentAttempt != null && currentAttempt.isSucc();
    }

    @Override
    public String toString() {
        return "TimerRequest{" +
            "offsetPy=" + offsetPy +
            ", sizePy=" + sizePy +
            ", delayTime=" + delayTime +
            ", enqueueTime=" + enqueueTime +
            ", magic=" + magic +
            ", msg=" + msg +
            ", attempt=" + attempt +
            ", pipelineAttempt=" + pipelineAttempt +
            ", deleteList=" + deleteList +
            '}';
    }

    /**
     * Completion state of one enqueue attempt.
     */
    public static class Attempt {

        private final CountDownLatch latch;
        private final AtomicBoolean released = new AtomicBoolean(false);
        private volatile boolean succ;
        private volatile boolean handedToPipeline;
        // Only attempts handed to the dequeue pipeline need a signal to wait on
        private volatile CountDownLatch done;

        Attempt(CountDownLatch latch) {
            this.latch = latch;
        }

        public void release(boolean succ) {
            this.succ = succ;
            if (released.compareAndSet(false, true)) {
                if (latch != null) {
                    latch.countDown();
                }
                CountDownLatch doneSignal = done;
                if (doneSignal != null) {
                    doneSignal.countDown();
                }
            }
        }

        public boolean isSucc() {
            return succ;
        }

        public boolean isDone() {
            CountDownLatch doneSignal = done;
            return doneSignal != null ? doneSignal.getCount() == 0 : released.get();
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            CountDownLatch doneSignal = done;
            return doneSignal != null ? doneSignal.await(timeout, unit) : released.get();
        }

        public void markHandedToPipeline() {
            if (done == null) {
                done = new CountDownLatch(1);
            }
            this.handedToPipeline = true;
        }

        public void clearHandedToPipeline() {
            this.handedToPipeline = false;
        }

        public boolean isHandedToPipeline() {
            return handedToPipeline;
        }

        @Override
        public String toString() {
            return "Attempt{" +
                "succ=" + succ +
                ", released=" + released +
                ", handedToPipeline=" + handedToPipeline +
                '}';
        }
    }
}
