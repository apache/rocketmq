package org.apache.rocketmq.auth.authentication.provider;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.config.ConfigRocksDBStorage;
import org.rocksdb.RocksIterator;

public class LocalAuthenticationMetadataProvider implements AuthenticationMetadataProvider {

    private ConfigRocksDBStorage storage;

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.storage = new ConfigRocksDBStorage(authConfig.getAuthConfigPath() + File.separator + "auth_user");
        if (!this.storage.start()) {
            throw new RuntimeException("Failed to load rocksdb for auth_user, please check whether it is occupied");
        }
    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        try {
            byte[] keyBytes = user.getUsername().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(user, SerializerFeature.BrowserCompatible);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
        } catch (Exception e) {
            throw new AuthenticationException("create user to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        try {
            this.storage.delete(username.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new AuthenticationException("delete user from RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        try {
            byte[] keyBytes = user.getUsername().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = JSON.toJSONBytes(user, SerializerFeature.BrowserCompatible);
            this.storage.put(keyBytes, keyBytes.length, valueBytes);
        } catch (Exception e) {
            throw new AuthenticationException("update user to RocksDB failed", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        try {
            byte[] keyBytes = username.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = this.storage.get(keyBytes);
            if (ArrayUtils.isEmpty(valueBytes)) {
                return CompletableFuture.completedFuture(null);
            }
            User user = JSON.parseObject(new String(valueBytes, StandardCharsets.UTF_8), User.class);
            return CompletableFuture.completedFuture(user);
        } catch (Exception e) {
            throw new AuthenticationException("get user from RocksDB failed", e);
        }
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        List<User> result = new ArrayList<>();
        try (RocksIterator iterator = this.storage.iterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String username = new String(iterator.key(), StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(filter) && !filter.contains(username)) {
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
    public void shutdown() {
        if (this.storage != null) {
            this.storage.shutdown();
        }
    }
}
