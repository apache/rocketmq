package org.apache.rocketmq.auth.authentication.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;

public interface AuthenticationMetadataManager {

    void shutdown();

    void initUser(AuthConfig authConfig);

    CompletableFuture<Void> createUser(User user);

    CompletableFuture<Void> updateUser(User user);

    CompletableFuture<Void> deleteUser(String username);

    CompletableFuture<User> getUser(String username);

    CompletableFuture<List<User>> listUser(String filter);

    CompletableFuture<Boolean> isSuperUser(String username);
}
