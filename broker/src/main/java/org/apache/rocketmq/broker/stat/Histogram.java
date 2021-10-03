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

package org.apache.rocketmq.broker.stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Histogram {
    private final List<AtomicInteger> frequency;
    private final List<String> labels;
    private final int capacity;
    private final String title;

    public Histogram(final String title, int capacity) {
        this.title = title;
        this.capacity = capacity;
        frequency = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            frequency.add(i, new AtomicInteger(0));
        }
        labels = new ArrayList<>();
    }

    public List<String> getLabels() {
        return labels;
    }

    public void countIn(int grade) {
        if (grade < 0) {
            return;
        }

        if (grade >= capacity) {
            frequency.get(capacity - 1).incrementAndGet();
            return;
        }

        frequency.get(grade).incrementAndGet();
    }

    public String reportAndReset() {
        StringBuilder sb = new StringBuilder(title);
        sb.append(": ");
        for (int i = 0; i < frequency.size(); i++) {
            if (0 != i) {
                sb.append(", ");
            }
            if (labels.size() > i) {
                sb.append(labels.get(i));
            }
            sb.append(frequency.get(i).getAndSet(0));
        }
        return sb.toString();
    }
}