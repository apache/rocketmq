/**
 * Copyright (C) 2010-2016 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.MixAll;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author MQDevelopers
 */
public class NamespaceUtilTest {

    private static final String INSTANCE_ID = "MQ_INST_XXX";
    private static final String INSTANCE_ID_WRONG = "MQ_INST_XXX1";
    private static final String TOPIC = "TOPIC_XXX";
    private static final String GROUP_ID = "GID_XXX";
    private static final String GROUP_ID_WITH_NAMESPACE = INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + GROUP_ID;
    private static final String TOPIC_WITH_NAMESPACE = INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + TOPIC;
    private static final String RETRY_TOPIC = MixAll.RETRY_GROUP_TOPIC_PREFIX + GROUP_ID;
    private static final String RETRY_TOPIC_WITH_NAMESPACE =
        MixAll.RETRY_GROUP_TOPIC_PREFIX + INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + GROUP_ID;

    @Test
    public void withoutNamespace() {
        String topic = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(topic, TOPIC);
        String topic1 = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(topic1, TOPIC);
        String groupId = NamespaceUtil.withoutNamespace(GROUP_ID_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(groupId, GROUP_ID);
        String groupId1 = NamespaceUtil.withoutNamespace(GROUP_ID_WITH_NAMESPACE);
        Assert.assertEquals(groupId1, GROUP_ID);
        String consumerId = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(consumerId, RETRY_TOPIC);
        String consumerId1 = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(consumerId1, RETRY_TOPIC);
        String consumerId2 = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE, INSTANCE_ID_WRONG);
        Assert.assertEquals(consumerId2, RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertNotEquals(consumerId2, RETRY_TOPIC);
    }
}