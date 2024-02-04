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

package org.apache.rocketmq.namesrv;

import java.util.Properties;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NamesrvStartupTest {

    @Mock
    private NamesrvController namesrvController;
    @Mock
    private Options options;

    @Before
    public void setUp() throws Exception {
        Mockito.when(namesrvController.initialize()).thenReturn(true);
    }

    @Test
    public void testStart() throws Exception {
        NamesrvController controller = NamesrvStartup.start(namesrvController);
        Assert.assertNotNull(controller);
    }

    @Test
    public void testShutdown() {
        NamesrvStartup.shutdown(namesrvController);
        Mockito.verify(namesrvController).shutdown();
    }

    @Test
    public void testBuildCommandlineOptions() {
        Options options = NamesrvStartup.buildCommandlineOptions(this.options);
        Assert.assertNotNull(options);
    }

    @Test
    public void testGetProperties() {
        Properties properties = NamesrvStartup.getProperties();
        Assert.assertNull(properties);
    }
}