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
