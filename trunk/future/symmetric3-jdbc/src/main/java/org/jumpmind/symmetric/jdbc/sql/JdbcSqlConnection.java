package org.jumpmind.symmetric.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.db.DbIntegrityViolationException;
import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.jdbc.db.IJdbcPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcPlatformFactory;

public class JdbcSqlConnection implements ISqlConnection {

    static final Log log = LogFactory.getLog(JdbcSqlConnection.class);

    protected DataSource dataSource;

    protected IJdbcPlatform platform;

    public JdbcSqlConnection(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public JdbcSqlConnection(IJdbcPlatform platform, DataSource dataSource) {
        this.dataSource = dataSource;
        this.platform = platform;
    }

    public IPlatform getPlatform() {
        if (this.platform == null) {
            this.platform = JdbcPlatformFactory.createPlatform(dataSource);
        }
        return this.platform;
    }

    public int queryForInt(String sql) {
        Number number = queryForObject(sql, Number.class);
        if (number != null) {
            return number.intValue();
        } else {
            return 0;
        }
    }

    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args) {
        return execute(new IConnectionCallback<T>() {
            @SuppressWarnings("unchecked")
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    StatementCreatorUtil.setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = (T) rs.getObject(1);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public int update(String sql) {
        return update(sql, null, null);
    }

    public int update(final String sql, final Object[] values, final int[] types) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                PreparedStatement ps = null;
                try {
                    ps = con.prepareStatement(sql);
                    if (values != null) {
                        StatementCreatorUtil.setValues(ps, values, types,
                                ((IJdbcPlatform) getPlatform()).getLobHandler());
                    }
                    return ps.executeUpdate();
                } finally {
                    close(ps);
                }
            }
        });
    }

    public int update(String... sql) {
        return update(true, true, -1, sql);
    }

    public int update(final boolean autoCommit, final boolean failOnError, final int commitRate,
            final String... sql) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                int updateCount = 0;
                boolean oldAutoCommitSetting = con.getAutoCommit();
                Statement stmt = null;
                try {
                    con.setAutoCommit(autoCommit);
                    stmt = con.createStatement();
                    int statementCount = 0;
                    for (String statement : sql) {
                        try {
                            updateCount += stmt.executeUpdate(statement);
                            statementCount++;
                            if (statementCount % commitRate == 0 && !autoCommit) {
                                con.commit();
                            }
                        } catch (SQLException ex) {
                            if (!failOnError) {
                                log.log(LogLevel.WARN, "%s.  Failed to execute: %s.",
                                        ex.getMessage(), sql);
                            }
                        }
                    }

                    if (!autoCommit) {
                        con.commit();
                    }
                    return updateCount;
                } catch (SQLException ex) {
                    if (!autoCommit) {
                        con.rollback();
                    }
                    throw ex;
                } finally {
                    close(stmt);
                    con.setAutoCommit(oldAutoCommitSetting);
                }
            }
        });
    }

    public void testConnection() {
        execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                return true;
            }
        });
    }

    public <T> T execute(IConnectionCallback<T> callback) {
        Connection c = null;
        try {
            c = dataSource.getConnection();
            return callback.execute(c);
        } catch (SQLException ex) {
            throw translate(ex);
        } finally {
            close(c);
        }
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException ex) {
        }
    }

    public DbException translate(Exception ex) {
        if (getPlatform().isDataIntegrityException(ex)) {
            return new DbIntegrityViolationException(ex);
        } else {
            return new DbException(ex);
        }
    }

}
