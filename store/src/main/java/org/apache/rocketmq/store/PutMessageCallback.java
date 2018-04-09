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

public class PutMessageCallback {
    private final Object waitObject = new Object();
    private volatile boolean completed = false;
    private PutMessageResult putMessageResult;

    protected void doAction(PutMessageResult putMessageResult) {
        //default empty
    }

    public void callback(PutMessageResult putMessageResult) {
        doAction(putMessageResult);
        this.putMessageResult = putMessageResult;
        completed = true;
        synchronized (waitObject) {
            waitObject.notifyAll();
        }
    }

    public void waitComplete() throws InterruptedException {
        waitComplete(-1);
    }

    public void waitComplete(long timeout) throws InterruptedException {
        synchronized (waitObject) {
            if (timeout < 0) {
                waitObject.wait();
            }
            else {
                waitObject.wait(timeout);
            }
        }
    }

    public boolean isCompleted() {
        return completed;
    }

    public PutMessageResult getPutMessageResult() {
        return putMessageResult;
    }
}
