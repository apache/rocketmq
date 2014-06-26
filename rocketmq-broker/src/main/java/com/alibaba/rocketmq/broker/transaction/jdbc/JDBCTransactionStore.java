package com.alibaba.rocketmq.broker.transaction.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;


public class JDBCTransactionStore implements TransactionStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
    private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
    private Connection connection;
    private AtomicLong totalRecordsValue = new AtomicLong(0);


    public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig) {
        this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
    }


    private boolean loadDriver() {
        try {
            Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
            log.info("Loaded the appropriate driver, {}",
                this.jdbcTransactionStoreConfig.getJdbcDriverClass());
            return true;
        }
        catch (Exception e) {
            log.info("Loaded the appropriate driver Exception", e);
        }

        return false;
    }


    private boolean computeTotalRecords() {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = this.connection.createStatement();

            resultSet = statement.executeQuery("select count(offset) as total from t_transaction");
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            this.totalRecordsValue.set(resultSet.getLong(1));
        }
        catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        }
        finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }

            if (null != resultSet) {
                try {
                    resultSet.close();
                }
                catch (SQLException e) {
                }
            }
        }

        return true;
    }


    private String createTableSql() {
        URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
        String fileContent = MixAll.file2String(resource);
        return fileContent;
    }


    private boolean createDB() {
        Statement statement = null;
        try {
            statement = this.connection.createStatement();

            String sql = this.createTableSql();
            log.info("createDB SQL:\n {}", sql);
            boolean execute = statement.execute(sql);
            log.info("createDB execute {}", execute ? "Success." : "Failed");
            return execute;
        }
        catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        }
        finally {
            if (null != statement) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                }
            }
        }
    }


    @Override
    public boolean open() {
        if (this.loadDriver()) {
            Properties props = new Properties();
            props.put("user", jdbcTransactionStoreConfig.getJdbcUser());
            props.put("password", jdbcTransactionStoreConfig.getJdbcPassword());

            try {
                this.connection =
                        DriverManager.getConnection(this.jdbcTransactionStoreConfig.getJdbcURL(), props);
                // 如果表不存在，尝试初始化表
                if (!this.computeTotalRecords()) {
                    return this.createDB();
                }

                return true;
            }
            catch (SQLException e) {
                log.info("Create JDBC Connection Exeption", e);
            }
        }

        return false;
    }


    @Override
    public void close() {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        }
        catch (SQLException e) {
        }
    }


    @Override
    public boolean write(TransactionRecord tr) {
        return false;
    }


    @Override
    public void remove(List<Long> pks) {
        // TODO Auto-generated method stub

    }


    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long totalRecords() {
        // TODO Auto-generated method stub
        return this.totalRecordsValue.get();
    }


    @Override
    public long minPK() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long maxPK() {
        // TODO Auto-generated method stub
        return 0;
    }

}
