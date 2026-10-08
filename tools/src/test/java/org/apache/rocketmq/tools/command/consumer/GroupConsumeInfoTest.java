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
package org.apache.rocketmq.tools.command.consumer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupConsumeInfoTest {

    private GroupConsumeInfo newGroup(String group, int count, long diffTotal) {
        GroupConsumeInfo info = new GroupConsumeInfo();
        info.setGroup(group);
        info.setCount(count);
        info.setDiffTotal(diffTotal);
        return info;
    }

    @Test
    public void equals_shouldBeReflexive() {
        GroupConsumeInfo info = newGroup("groupA", 10, 100L);
        assertThat(info.equals(info)).isTrue();
    }

    @Test
    public void equals_shouldBeSymmetric() {
        GroupConsumeInfo a = newGroup("groupA", 10, 100L);
        GroupConsumeInfo b = newGroup("groupA", 10, 100L);
        assertThat(a.equals(b)).isTrue();
        assertThat(b.equals(a)).isTrue();
    }

    @Test
    public void equals_shouldBeTransitive() {
        GroupConsumeInfo a = newGroup("groupA", 10, 100L);
        GroupConsumeInfo b = newGroup("groupA", 20, 200L);
        GroupConsumeInfo c = newGroup("groupA", 30, 300L);
        assertThat(a.equals(b)).isTrue();
        assertThat(b.equals(c)).isTrue();
        assertThat(a.equals(c)).isTrue();
    }

    @Test
    public void equals_shouldBeConsistent() {
        GroupConsumeInfo a = newGroup("groupA", 10, 100L);
        GroupConsumeInfo b = newGroup("groupA", 10, 100L);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.equals(b)).isTrue();
    }

    @Test
    public void equals_shouldReturnFalseForNull() {
        GroupConsumeInfo info = newGroup("groupA", 10, 100L);
        assertThat(info.equals(null)).isFalse();
    }

    @Test
    public void equals_shouldReturnFalseForDifferentType() {
        GroupConsumeInfo info = newGroup("groupA", 10, 100L);
        assertThat(info.equals("groupA")).isFalse();
    }

    @Test
    public void equals_shouldReturnFalseForDifferentGroup() {
        GroupConsumeInfo a = newGroup("groupA", 10, 100L);
        GroupConsumeInfo b = newGroup("groupB", 10, 100L);
        assertThat(a.equals(b)).isFalse();
    }

    @Test
    public void equals_shouldHandleNullGroup() {
        GroupConsumeInfo a = newGroup(null, 10, 100L);
        GroupConsumeInfo b = newGroup(null, 10, 100L);
        GroupConsumeInfo c = newGroup("groupA", 10, 100L);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.equals(c)).isFalse();
    }

    @Test
    public void hashCode_shouldBeConsistentWithEquals() {
        GroupConsumeInfo a = newGroup("groupA", 10, 100L);
        GroupConsumeInfo b = newGroup("groupA", 20, 200L);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    public void hashCode_shouldBeConsistentAcrossInvocations() {
        GroupConsumeInfo info = newGroup("groupA", 10, 100L);
        int first = info.hashCode();
        int second = info.hashCode();
        assertThat(first).isEqualTo(second);
    }
}
