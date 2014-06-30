package com.alibaba.rocketmq.broker.transaction.jdbc;

public class JDBCTransactionStoreConfig {
    private String jdbcDriverClass = "com.mysql.jdbc.Driver";
    private String jdbcURL =
            "jdbc:mysql://10.235.170.23:3306/ons_console?useUnicode=true&characterEncoding=UTF-8";
    private String jdbcUser = "rocketmq";
    private String jdbcPassword = "taobao.com";


    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }


    public void setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
    }


    public String getJdbcURL() {
        return jdbcURL;
    }


    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }


    public String getJdbcUser() {
        return jdbcUser;
    }


    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }


    public String getJdbcPassword() {
        return jdbcPassword;
    }


    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }
}
