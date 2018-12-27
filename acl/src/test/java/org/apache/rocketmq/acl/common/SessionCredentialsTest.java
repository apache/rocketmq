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
package org.apache.rocketmq.acl.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class SessionCredentialsTest {

    @Test
    public void equalsTest(){
        SessionCredentials sessionCredentials=new SessionCredentials("RocketMQ","12345678");
        sessionCredentials.setSecurityToken("abcd");
        SessionCredentials other=new SessionCredentials("RocketMQ","12345678","abcd");
        Assert.assertTrue(sessionCredentials.equals(other));
    }

    @Test
    public void updateContentTest(){
        SessionCredentials sessionCredentials=new SessionCredentials();
        Properties properties=new Properties();
        properties.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredentials.updateContent(properties);
    }


}
