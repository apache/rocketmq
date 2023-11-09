package org.apache.rocketmq.auth.helper;

import org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider;
import org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider;
import org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;

public class AuthorizationTestHelper {

    public static AuthConfig createDefaultConfig() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("test");
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationProvider(DefaultAuthenticationProvider.class.getName());
        authConfig.setAuthenticationMetadataProvider(LocalAuthenticationMetadataProvider.class.getName());
        authConfig.setAuthorizationEnabled(true);
        authConfig.setAuthorizationProvider(DefaultAuthorizationProvider.class.getName());
        authConfig.setAuthorizationMetadataProvider(LocalAuthorizationMetadataProvider.class.getName());
        return authConfig;
    }
}
