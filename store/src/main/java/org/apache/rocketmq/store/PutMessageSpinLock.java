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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Spin lock Implementation to put message
 */
public class PutMessageSpinLock implements PutMessageLock {

    static final class Node {
        /**
         * True: Signal, false : Wait.
         */
        private final AtomicBoolean waitStatus = new AtomicBoolean(false);

        /**
         * Link to predecessor node that current node/thread relies on
         * for checking waitStatus.
         */
        volatile Node prev;

        Node() { }
    }

    /**
     * Tail of the wait queue
     */
    private  final AtomicReference<Node> tail = new AtomicReference<>();

    /**
     * Head of the wait queue
     */
    private  final AtomicReference<Node> head = new AtomicReference<>();

    /**
     * true: can acquire, false: can not acquire
     */
    private final AtomicBoolean status = new AtomicBoolean(true);

    /**
     * Inserts node into queue, initializing if necessary
     * @param node the node to insert
     * @return node's predecessor
     */
    private Node enq(final Node node) {
        for (;;) {
            Node t = tail.get();
            if (t == null) { // Must initialize
                Node initNode = new Node();
                initNode.waitStatus.set(true);
                if (head.compareAndSet(null, initNode)) {
                    tail.set(initNode);
                }
            } else {
                node.prev = t;
                if (tail.compareAndSet(t, node)) {
                    return t;
                }
            }
        }
    }

    /**
     * Creates and enqueues node for current thread.
     *
     * @return the new node
     */
    private Node addWaiter() {
        Node node = new Node();
        // Try the fast path of enq; backup to full enq on failure
        Node pred = tail.get();
        if (pred != null) {
            node.prev = pred;
            if (tail.compareAndSet(pred, node)) {
                return node;
            }
        }
        enq(node);
        return node;
    }

    /**
     * Sets head of queue to be node, thus dequeuing.
     * Also nulls out unused fields for sake of GC
     * @param node the node
     */
    private void setHead(Node node) {
        head.set(node);
        node.prev = null;
    }

    @Override
    public void lock() {
        if (!status.compareAndSet(true, false)) {
            Node node = addWaiter();
            Node pred = node.prev;
            AtomicBoolean preStatus = pred.waitStatus;
            for (;;) {
                if (preStatus.get() && status.compareAndSet(true, false)) {
                    setHead(node);
                    return;
                }
            }
        }
    }

    @Override
    public void unlock() {
        status.set(true);
        Node h = head.get();
        if (h != null) {
            /**
             * notify next waiter node
             */
            AtomicBoolean headStatus = h.waitStatus;
            headStatus.set(true);
        }
    }
}
