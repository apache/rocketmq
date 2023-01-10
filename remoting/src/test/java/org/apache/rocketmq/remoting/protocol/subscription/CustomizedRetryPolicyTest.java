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

package org.apache.rocketmq.remoting.protocol.subscription;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomizedRetryPolicyTest {

    @Test
    public void testNextDelayDuration() {
        CustomizedRetryPolicy customizedRetryPolicy = new CustomizedRetryPolicy();
        long actual = customizedRetryPolicy.nextDelayDuration(0);
        assertThat(actual).isEqualTo(TimeUnit.SECONDS.toMillis(10));
        actual = customizedRetryPolicy.nextDelayDuration(10);
        assertThat(actual).isEqualTo(TimeUnit.MINUTES.toMillis(9));
    }

    @Test
    public void testNextDelayDurationOutOfRange() {
        CustomizedRetryPolicy customizedRetryPolicy = new CustomizedRetryPolicy();
        long actual = customizedRetryPolicy.nextDelayDuration(-1);
        assertThat(actual).isEqualTo(TimeUnit.SECONDS.toMillis(10));
        actual = customizedRetryPolicy.nextDelayDuration(100);
        assertThat(actual).isEqualTo(TimeUnit.HOURS.toMillis(2));
    }
}
