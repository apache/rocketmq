package com.alibaba.rocketmq.test.integration;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


/**
 * 数据库操作
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-4-24
 */
public class DaoService {
    private static final String JDBC_DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final int QUERY_TIMEOUT = 3;
    private static JdbcTemplate jt;

    static {
        FileInputStream fis = null;
        try {
            File file = new File(DaoService.class.getResource("/jdbc.properties").toURI());
            Properties props = new Properties();
            props.load(fis = new FileInputStream(file));

            BasicDataSource ds = new BasicDataSource();
            ds.setDriverClassName(JDBC_DRIVER_NAME);
            ds.setUrl(props.getProperty("db.url").trim());
            ds.setUsername(props.getProperty("db.user").trim());
            ds.setPassword(props.getProperty("db.password").trim());
            ds.setInitialSize(10);
            ds.setMaxActive(20);
            ds.setMaxIdle(50);
            ds.setMaxWait(3000L);
            ds.setPoolPreparedStatements(true);

            jt = new JdbcTemplate();
            jt.setMaxRows(50000); // 设置最大记录数，防止内存膨胀
            jt.setQueryTimeout(QUERY_TIMEOUT);
            jt.setDataSource(ds);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(0);

        }
        finally {
            if (null != fis) {
                try {
                    fis.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void insert(String msgId) {
        try {
            int result = jt.update("replace into msg_check(msg_id) values(?)", msgId);
            System.out.println(String.format("Insert OK, %s rows affected.", result));
        }
        catch (CannotGetJdbcConnectionException e) {
            throw e;
        }
    }


    public void delete(String msgId) {
        try {
            int result = jt.update("delete from msg_check where msg_id=?", msgId);
            System.out.println(String.format("Delete OK, %s rows affected.", result));
        }
        catch (CannotGetJdbcConnectionException e) {
            throw e;
        }
    }


    public static void main(String[] args) {
        DaoService dao = new DaoService();
        dao.delete("body");
    }
}
