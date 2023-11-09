package org.apache.rocketmq.acl2.manager;

import java.util.List;
import org.apache.rocketmq.acl2.model.User;

public interface UserManager {

    void createUser(User user);

    void deleteUser(String username);

    void updateUser(User user);

    User getUser(String username);

    List<User> listUsers();
}
