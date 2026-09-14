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
package org.apache.rocketmq.broker.auth.converter;

import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UserConverterTest {

    @Test
    public void outboundConversionOmitsPasswordByDefault() {
        UserInfo result = UserConverter.convertUser(
            User.of("user", "secret", UserType.NORMAL));

        assertThat(result.getUsername()).isEqualTo("user");
        assertThat(result.getPassword()).isNull();
        assertThat(result.getUserType()).isEqualTo("Normal");
    }

    @Test
    public void inboundConversionRetainsPasswordForAuthentication() {
        User result = UserConverter.convertUser(
            UserInfo.of("user", "secret", "Normal"));

        assertThat(result.getUsername()).isEqualTo("user");
        assertThat(result.getPassword()).isEqualTo("secret");
        assertThat(result.getUserType()).isEqualTo(UserType.NORMAL);
    }
}
