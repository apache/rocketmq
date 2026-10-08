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
package org.apache.rocketmq.tools.command.message;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.tools.command.message.PrintMessageByQueueCommand.TagCountBean;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TagCountBeanTest {

    private TagCountBean newBean(String tag, long count) {
        return new TagCountBean(tag, new AtomicLong(count));
    }

    @Test
    public void equals_shouldBeReflexive() {
        TagCountBean bean = newBean("tagA", 10L);
        assertThat(bean.equals(bean)).isTrue();
    }

    @Test
    public void equals_shouldBeSymmetric() {
        TagCountBean a = newBean("tagA", 10L);
        TagCountBean b = newBean("tagB", 10L);
        assertThat(a.equals(b)).isTrue();
        assertThat(b.equals(a)).isTrue();
    }

    @Test
    public void equals_shouldBeTransitive() {
        TagCountBean a = newBean("tagA", 10L);
        TagCountBean b = newBean("tagB", 10L);
        TagCountBean c = newBean("tagC", 10L);
        assertThat(a.equals(b)).isTrue();
        assertThat(b.equals(c)).isTrue();
        assertThat(a.equals(c)).isTrue();
    }

    @Test
    public void equals_shouldBeConsistent() {
        TagCountBean a = newBean("tagA", 10L);
        TagCountBean b = newBean("tagA", 10L);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.equals(b)).isTrue();
    }

    @Test
    public void equals_shouldReturnFalseForNull() {
        TagCountBean bean = newBean("tagA", 10L);
        assertThat(bean.equals(null)).isFalse();
    }

    @Test
    public void equals_shouldReturnFalseForDifferentType() {
        TagCountBean bean = newBean("tagA", 10L);
        assertThat(bean.equals("tagA")).isFalse();
    }

    @Test
    public void equals_shouldReturnFalseForDifferentCount() {
        TagCountBean a = newBean("tagA", 10L);
        TagCountBean b = newBean("tagA", 20L);
        assertThat(a.equals(b)).isFalse();
    }

    @Test
    public void hashCode_shouldBeConsistentWithEquals() {
        TagCountBean a = newBean("tagA", 10L);
        TagCountBean b = newBean("tagB", 10L);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    public void hashCode_shouldBeConsistentAcrossInvocations() {
        TagCountBean bean = newBean("tagA", 10L);
        int first = bean.hashCode();
        int second = bean.hashCode();
        assertThat(first).isEqualTo(second);
    }

    @Test
    public void compareTo_shouldOrderByCountDescending() {
        TagCountBean small = newBean("tagA", 1L);
        TagCountBean large = newBean("tagB", 100L);
        assertThat(small.compareTo(large)).isPositive();
        assertThat(large.compareTo(small)).isNegative();
        assertThat(large.compareTo(newBean("tagC", 100L))).isZero();
    }
}
