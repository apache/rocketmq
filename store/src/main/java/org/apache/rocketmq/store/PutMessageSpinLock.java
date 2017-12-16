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
         * True: Wait signal, false : In lock.
         */
        private final AtomicBoolean waitStatus = new AtomicBoolean(true);

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
     * Inserts node into queue, initializing if necessary
     * @param node the node to insert
     * @return node's predecessor
     */
    private Node enq(final Node node) {
        for (;;) {
            Node t = tail.get();
            if (t == null) { // Must initialize
                Node initNode = new Node();
                initNode.waitStatus.set(false);
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
        Node node = new Node();
        Node pred = enq(node);
        for (;;) {
            if (!pred.waitStatus.get()) {
                setHead(node);
                return;
            }
        }
    }

    @Override
    public void unlock() {
        Node h = head.get();
        h.waitStatus.compareAndSet(true, false);
    }
}
