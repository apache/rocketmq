package org.apache.rocketmq.auth.authentication.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class AuthenticationMetadataManagerImpl implements AuthenticationMetadataManager {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    private final AuthorizationMetadataProvider authorizationMetadataProvider;

    public AuthenticationMetadataManagerImpl(AuthConfig authConfig) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(authConfig);
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(authConfig);
    }

    @Override
    public void shutdown() {
        if (this.authenticationMetadataProvider != null) {
            this.authenticationMetadataProvider.shutdown();
        }
        if (this.authorizationMetadataProvider != null) {
            this.authorizationMetadataProvider.shutdown();
        }
    }

    @Override
    public CompletableFuture<Void> initUser(User user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(user, true);
            user.setUserType(UserType.SUPER);
            result = this.getAuthenticationMetadataProvider().hasUser().thenCompose(hasUser -> {
                if (hasUser) {
                    throw new AuthenticationException("The broker has initialized users");
                }
                return this.getAuthenticationMetadataProvider().createUser(user);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(user, true);
            if (user.getUserType() == null) {
                user.setUserType(UserType.NORMAL);
            }
            result = this.getAuthenticationMetadataProvider().getUser(user.getUsername()).thenCompose(old -> {
                if (old != null) {
                    throw new AuthenticationException("The user is existed");
                }
                return this.getAuthenticationMetadataProvider().createUser(user);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(user, false);
            result = this.getAuthenticationMetadataProvider().getUser(user.getUsername()).thenCompose(old -> {
                if (old == null) {
                    throw new AuthenticationException("The user is not exist");
                }
                if (StringUtils.isNotBlank(user.getPassword())) {
                    old.setPassword(user.getPassword());
                }
                if (user.getUserType() != null) {
                    old.setUserType(user.getUserType());
                }
                return this.getAuthenticationMetadataProvider().updateUser(old);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(username)) {
                throw new AuthenticationException("username can not be blank");
            }
            CompletableFuture<Void> deleteUser = this.getAuthenticationMetadataProvider().deleteUser(username);
            CompletableFuture<Void> deleteAcl = this.getAuthorizationMetadataProvider().deleteAcl(User.of(username));
            return CompletableFuture.allOf(deleteUser, deleteAcl);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        CompletableFuture<User> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(username)) {
                throw new AuthenticationException("username can not be blank");
            }
            result = this.getAuthenticationMetadataProvider().getUser(username);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        CompletableFuture<List<User>> result = new CompletableFuture<>();
        try {
            result = this.getAuthenticationMetadataProvider().listUser(filter);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String username) {
        return this.getUser(username).thenApply(user -> {
            if (user == null) {
                throw new AuthenticationException("user not found. username:{}", username);
            }
            return user.getUserType() == UserType.SUPER;
        });
    }

    private void validate(User user, boolean isCreate) {
        if (user == null) {
            throw new AuthenticationException("user can not be null");
        }
        if (StringUtils.isBlank(user.getUsername())) {
            throw new AuthenticationException("username can not be blank");
        }
        if (isCreate && StringUtils.isBlank(user.getPassword())) {
            throw new AuthenticationException("password can not be blank");
        }
    }

    private void handleException(Exception e, CompletableFuture<?> result) {
        Throwable throwable = ExceptionUtils.getRealException(e);
        if (throwable instanceof AuthenticationException) {
            result.completeExceptionally(throwable);
        } else {
            result.completeExceptionally(new AuthenticationException(ResponseCode.SYSTEM_ERROR, throwable));
        }
    }

    private AuthorizationMetadataProvider getAuthorizationMetadataProvider() {
        if (authenticationMetadataProvider == null) {
            throw new IllegalStateException("The authenticationMetadataProvider is not configured");
        }
        return authorizationMetadataProvider;
    }

    private AuthenticationMetadataProvider getAuthenticationMetadataProvider() {
        if (authorizationMetadataProvider == null) {
            throw new IllegalStateException("The authorizationMetadataProvider is not configured");
        }
        return authenticationMetadataProvider;
    }
}
