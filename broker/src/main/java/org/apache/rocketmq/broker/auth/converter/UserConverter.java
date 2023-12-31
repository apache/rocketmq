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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;

public class UserConverter {

    public static List<UserInfo> convertUsers(List<User> users) {
        return users.stream().map(UserConverter::convertUser)
            .collect(Collectors.toList());
    }

    public static UserInfo convertUser(User user) {
        UserInfo result = new UserInfo();
        result.setUsername(user.getUsername());
        result.setPassword(user.getPassword());
        if (user.getUserType() != null) {
            result.setUserType(user.getUserType().getName());
        }
        return result;
    }

    public static User convertUser(UserInfo userInfo) {
        User result = new User();
        result.setUsername(userInfo.getUsername());
        result.setPassword(userInfo.getPassword());
        result.setUserType(UserType.getByName(userInfo.getUserType()));
        return result;
    }
}
