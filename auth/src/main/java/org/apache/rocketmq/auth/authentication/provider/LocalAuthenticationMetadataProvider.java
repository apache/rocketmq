package org.apache.rocketmq.auth.authentication.provider;

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rocksdb.RocksIterator;

public class LocalAuthenticationMetadataProvider implements AuthenticationMetadataProvider {

    private ConfigRocksDBStorage storage;

    private LoadingCache<String, User> userCache;

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.storage = new ConfigRocksDBStorage(authConfig.getAuthConfigPath() + File.separator + "auth_user");
        if (!this.storage.start()) {
            throw new RuntimeException("Failed to load rocksdb for auth_user, please check whether it is occupied");
        }

        ThreadPoolExecutor cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            1,
            1,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "UserCacheRefresh",
            100000
        );

        this.userCache = Caffeine.newBuilder()
            .maximumSize(authConfig.getUserCacheMaxNum())
            .expireAfterAccess(authConfig.getUserCacheExpiredSecond(), TimeUnit.SECONDS)
            .refreshAfterWrite(authConfig.getUserCacheRefreshSecond(), TimeUnit.SECONDS)
            .executor(cacheRefreshExecutor)
            .build(new UserCacheLoader(this.storage));
    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        try {
            byte[] keyBytes = user.getUsername().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(user);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.userCache.put(user.getUsername(), user);
        } catch (Exception e) {
            throw new AuthenticationException("create user to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        try {
            this.storage.delete(username.getBytes(StandardCharsets.UTF_8));
            this.userCache.invalidate(username);
        } catch (Exception e) {
            throw new AuthenticationException("delete user from RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        try {
            byte[] keyBytes = user.getUsername().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(user);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
            this.userCache.put(user.getUsername(), user);
        } catch (Exception e) {
            throw new AuthenticationException("update user to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        User user = this.userCache.get(username);
        if (user == UserCacheLoader.EMPTY_USER) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(user);
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        List<User> result = new ArrayList<>();
        try (RocksIterator iterator = this.storage.iterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String username = new String(iterator.key(), StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(filter) && !username.contains(filter)) {
                    iterator.next();
                    continue;
                }
                User user = JSON.parseObject(new String(iterator.value(), StandardCharsets.UTF_8), User.class);
                result.add(user);
                iterator.next();
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Boolean> hasUser() {
        try (RocksIterator iterator = this.storage.iterator()) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                return CompletableFuture.completedFuture(true);
            }
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public void shutdown() {
        if (this.storage != null) {
            this.storage.shutdown();
        }
    }

    private static class UserCacheLoader implements CacheLoader<String, User> {
        private final ConfigRocksDBStorage storage;
        public static final User EMPTY_USER = new User();

        public UserCacheLoader(ConfigRocksDBStorage storage) {
            this.storage = storage;
        }

        @Override
        public User load(@NonNull String username) {
            try {
                byte[] keyBytes = username.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = storage.get(keyBytes);
                if (ArrayUtils.isEmpty(valueBytes)) {
                    return EMPTY_USER;
                }
                return JSON.parseObject(new String(valueBytes, StandardCharsets.UTF_8), User.class);
            } catch (Exception e) {
                throw new AuthenticationException("get user from RocksDB failed", e);
            }
        }
    }
}
