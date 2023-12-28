package org.apache.rocketmq.auth.config;

public class AuthConfig implements Cloneable {

    private String configName;

    private String clusterName;

    private String authConfigPath;

    private boolean authenticationEnabled = false;

    private String authenticationProvider;

    private String authenticationMetadataProvider;

    private String authenticationWhitelist;

    private String initAuthenticationUser;

    private String innerClientAuthenticationCredentials;

    private boolean authorizationEnabled = false;

    private String authorizationProvider;

    private String authorizationMetadataProvider;

    private String authorizationWhitelist;

    private boolean migrateFromAclV1Enabled = false;

    private int userCacheMaxNum = 1000;

    private int userCacheExpiredSecond = 600;

    private int userCacheRefreshSecond = 60;

    private int aclCacheMaxNum = 1000;

    private int aclCacheExpiredSecond = 600;

    private int aclCacheRefreshSecond = 60;

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

    public String getAuthenticationWhitelist() {
        return authenticationWhitelist;
    }

    public void setAuthenticationWhitelist(String authenticationWhitelist) {
        this.authenticationWhitelist = authenticationWhitelist;
    }

    public String getInitAuthenticationUser() {
        return initAuthenticationUser;
    }

    public void setInitAuthenticationUser(String initAuthenticationUser) {
        this.initAuthenticationUser = initAuthenticationUser;
    }

    public String getInnerClientAuthenticationCredentials() {
        return innerClientAuthenticationCredentials;
    }

    public void setInnerClientAuthenticationCredentials(String innerClientAuthenticationCredentials) {
        this.innerClientAuthenticationCredentials = innerClientAuthenticationCredentials;
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

    public String getAuthorizationWhitelist() {
        return authorizationWhitelist;
    }

    public void setAuthorizationWhitelist(String authorizationWhitelist) {
        this.authorizationWhitelist = authorizationWhitelist;
    }

    public boolean isMigrateFromAclV1Enabled() {
        return migrateFromAclV1Enabled;
    }

    public void setMigrateFromAclV1Enabled(boolean migrateFromAclV1Enabled) {
        this.migrateFromAclV1Enabled = migrateFromAclV1Enabled;
    }

    public int getUserCacheMaxNum() {
        return userCacheMaxNum;
    }

    public void setUserCacheMaxNum(int userCacheMaxNum) {
        this.userCacheMaxNum = userCacheMaxNum;
    }

    public int getUserCacheExpiredSecond() {
        return userCacheExpiredSecond;
    }

    public void setUserCacheExpiredSecond(int userCacheExpiredSecond) {
        this.userCacheExpiredSecond = userCacheExpiredSecond;
    }

    public int getUserCacheRefreshSecond() {
        return userCacheRefreshSecond;
    }

    public void setUserCacheRefreshSecond(int userCacheRefreshSecond) {
        this.userCacheRefreshSecond = userCacheRefreshSecond;
    }

    public int getAclCacheMaxNum() {
        return aclCacheMaxNum;
    }

    public void setAclCacheMaxNum(int aclCacheMaxNum) {
        this.aclCacheMaxNum = aclCacheMaxNum;
    }

    public int getAclCacheExpiredSecond() {
        return aclCacheExpiredSecond;
    }

    public void setAclCacheExpiredSecond(int aclCacheExpiredSecond) {
        this.aclCacheExpiredSecond = aclCacheExpiredSecond;
    }

    public int getAclCacheRefreshSecond() {
        return aclCacheRefreshSecond;
    }

    public void setAclCacheRefreshSecond(int aclCacheRefreshSecond) {
        this.aclCacheRefreshSecond = aclCacheRefreshSecond;
    }
}
