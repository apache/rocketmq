package org.apache.rocketmq.auth.authentication.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.auth.authentication.model.User;

public interface AuthenticationMetadataManager {

    void shutdown();

    CompletableFuture<Void> initUser(User user);

    CompletableFuture<Void> createUser(User user);

    CompletableFuture<Void> updateUser(User user);

    CompletableFuture<Void> deleteUser(String username);

    CompletableFuture<User> getUser(String username);

    CompletableFuture<List<User>> listUser(String filter);

    CompletableFuture<Boolean> isSuperUser(String username);
}
