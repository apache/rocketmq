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
package org.apache.rocketmq.remoting.protocol.header;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetConsumeStatsRequestHeaderTest {

    private GetConsumeStatsRequestHeader header;

    @Before
    public void setUp() {
        header = new GetConsumeStatsRequestHeader();
    }

    @Test
    public void updateTopicList_NullTopicList_DoesNotUpdate() {
        header.updateTopicList(null);
        assertNull(header.getTopicList());
    }

    @Test
    public void updateTopicList_EmptyTopicList_SetsEmptyString() {
        header.updateTopicList(Collections.emptyList());
        assertNull(header.getTopicList());
    }

    @Test
    public void updateTopicList_SingleTopic_SetsSingleTopicString() {
        List<String> topicList = Collections.singletonList("TopicA");
        header.updateTopicList(topicList);
        assertEquals("TopicA;", header.getTopicList());
    }

    @Test
    public void updateTopicList_MultipleTopics_SetsMultipleTopicsString() {
        List<String> topicList = Arrays.asList("TopicA", "TopicB", "TopicC");
        header.updateTopicList(topicList);
        assertEquals("TopicA;TopicB;TopicC;", header.getTopicList());
    }

    @Test
    public void updateTopicList_RepeatedTopics_SetsRepeatedTopicsString() {
        List<String> topicList = Arrays.asList("TopicA", "TopicA", "TopicB");
        header.updateTopicList(topicList);
        assertEquals("TopicA;TopicA;TopicB;", header.getTopicList());
    }

    @Test
    public void fetchTopicList_NullTopicList_ReturnsEmptyList() {
        header.setTopicList(null);
        List<String> topicList = header.fetchTopicList();
        assertEquals(Collections.emptyList(), topicList);

        header.updateTopicList(new ArrayList<>());
        topicList = header.fetchTopicList();
        assertEquals(Collections.emptyList(), topicList);
    }

    @Test
    public void fetchTopicList_EmptyTopicList_ReturnsEmptyList() {
        header.setTopicList("");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Collections.emptyList(), topicList);
    }

    @Test
    public void fetchTopicList_BlankTopicList_ReturnsEmptyList() {
        header.setTopicList("   ");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Collections.emptyList(), topicList);
    }

    @Test
    public void fetchTopicList_SingleTopic_ReturnsSingleTopicList() {
        header.setTopicList("TopicA");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Collections.singletonList("TopicA"), topicList);
    }

    @Test
    public void fetchTopicList_MultipleTopics_ReturnsTopicList() {
        header.setTopicList("TopicA;TopicB;TopicC");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Arrays.asList("TopicA", "TopicB", "TopicC"), topicList);
    }

    @Test
    public void fetchTopicList_TopicListEndsWithSeparator_ReturnsTopicList() {
        header.setTopicList("TopicA;TopicB;");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Arrays.asList("TopicA", "TopicB"), topicList);
    }

    @Test
    public void fetchTopicList_TopicListStartsWithSeparator_ReturnsTopicList() {
        header.setTopicList(";TopicA;TopicB");
        List<String> topicList = header.fetchTopicList();
        assertEquals(Arrays.asList("TopicA", "TopicB"), topicList);
    }
}
