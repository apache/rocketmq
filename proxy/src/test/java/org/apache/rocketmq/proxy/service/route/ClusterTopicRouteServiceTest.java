/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.route;

import com.google.common.net.HostAndPort;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ClusterTopicRouteServiceTest extends BaseServiceTest {

    private ClusterTopicRouteService topicRouteService;

    @Before
    public void before() throws Throwable {
        super.before();
        this.topicRouteService = new ClusterTopicRouteService(this.mqClientAPIFactory);

        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong())).thenReturn(topicRouteData);
        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(ERR_TOPIC), anyLong())).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
    }

    @Test
    public void testGetCurrentMessageQueueView() throws Throwable {
        MQClientException exception = catchThrowableOfType(() -> this.topicRouteService.getCurrentMessageQueueView(ERR_TOPIC), MQClientException.class);
        assertTrue(TopicRouteHelper.isTopicNotExistError(exception));
        assertEquals(1, this.topicRouteService.topicCache.asMap().size());

        assertNotNull(this.topicRouteService.getCurrentMessageQueueView(TOPIC));
        assertEquals(2, this.topicRouteService.topicCache.asMap().size());
    }

    @Test
    public void testGetTopicRouteForProxy() throws Throwable {
        List<Address> addressList = Lists.newArrayList(new Address(Address.AddressScheme.IPv4, HostAndPort.fromParts("127.0.0.1", 8888)));
        ProxyTopicRouteData proxyTopicRouteData = this.topicRouteService.getTopicRouteForProxy(addressList, TOPIC);

        assertEquals(1, proxyTopicRouteData.getBrokerDatas().size());
        assertEquals(addressList, proxyTopicRouteData.getBrokerDatas().get(0).getBrokerAddrs().get(MixAll.MASTER_ID));
    }
}