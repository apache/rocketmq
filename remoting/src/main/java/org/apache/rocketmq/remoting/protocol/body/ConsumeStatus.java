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

package org.apache.rocketmq.remoting.protocol.body;

public class ConsumeStatus {
    private double pullRT;
    private double pullTPS;
    private double consumeRT;
    private double consumeOKTPS;
    private double consumeFailedTPS;

    private long consumeFailedMsgs;

    public double getPullRT() {
        return pullRT;
    }

    public void setPullRT(double pullRT) {
        this.pullRT = pullRT;
    }

    public double getPullTPS() {
        return pullTPS;
    }

    public void setPullTPS(double pullTPS) {
        this.pullTPS = pullTPS;
    }

    public double getConsumeRT() {
        return consumeRT;
    }

    public void setConsumeRT(double consumeRT) {
        this.consumeRT = consumeRT;
    }

    public double getConsumeOKTPS() {
        return consumeOKTPS;
    }

    public void setConsumeOKTPS(double consumeOKTPS) {
        this.consumeOKTPS = consumeOKTPS;
    }

    public double getConsumeFailedTPS() {
        return consumeFailedTPS;
    }

    public void setConsumeFailedTPS(double consumeFailedTPS) {
        this.consumeFailedTPS = consumeFailedTPS;
    }

    public long getConsumeFailedMsgs() {
        return consumeFailedMsgs;
    }

    public void setConsumeFailedMsgs(long consumeFailedMsgs) {
        this.consumeFailedMsgs = consumeFailedMsgs;
    }
}
