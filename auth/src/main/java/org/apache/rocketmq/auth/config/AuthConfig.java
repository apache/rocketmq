package org.apache.rocketmq.auth.config;

public class AuthConfig implements Cloneable {

    private String configName;

    private String clusterName;

    private String authConfigPath;

    private boolean authenticationEnabled = false;

    private String authenticationProvider;

    private String authenticationMetadataProvider;

    private boolean authorizationEnabled = false;

    private String authorizationProvider;

    private String authorizationMetadataProvider;

    @Override
    public AuthConfig clone() {
        try {
            return (AuthConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getAuthConfigPath() {
        return authConfigPath;
    }

    public void setAuthConfigPath(String authConfigPath) {
        this.authConfigPath = authConfigPath;
    }

    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }

    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
    }

    public String getAuthenticationProvider() {
        return authenticationProvider;
    }

    public void setAuthenticationProvider(String authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    public String getAuthenticationMetadataProvider() {
        return authenticationMetadataProvider;
    }

    public void setAuthenticationMetadataProvider(String authenticationMetadataProvider) {
        this.authenticationMetadataProvider = authenticationMetadataProvider;
    }

    public boolean isAuthorizationEnabled() {
        return authorizationEnabled;
    }

    public void setAuthorizationEnabled(boolean authorizationEnabled) {
        this.authorizationEnabled = authorizationEnabled;
    }

    public String getAuthorizationProvider() {
        return authorizationProvider;
    }

    public void setAuthorizationProvider(String authorizationProvider) {
        this.authorizationProvider = authorizationProvider;
    }

    public String getAuthorizationMetadataProvider() {
        return authorizationMetadataProvider;
    }

    public void setAuthorizationMetadataProvider(String authorizationMetadataProvider) {
        this.authorizationMetadataProvider = authorizationMetadataProvider;
    }
}
