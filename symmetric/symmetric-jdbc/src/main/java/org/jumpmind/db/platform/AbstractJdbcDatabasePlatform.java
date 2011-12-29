package org.jumpmind.db.platform;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.AbstractDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.jumpmind.log.Log;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;
    
    protected DatabasePlatformSettings settings;
    
    public AbstractJdbcDatabasePlatform(DataSource dataSource, DatabasePlatformSettings settings, Log log) {
        super(log);
        this.dataSource = dataSource;
        this.settings = settings;
        createSqlTemplate();
    }
    
    protected void createSqlTemplate() {
        this.sqlTemplate = new JdbcSqlTemplate(dataSource, settings, null);        
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }
    
    public boolean isPrimaryKeyViolation(Exception ex) {
        boolean primaryKeyViolation = false;
        if (primaryKeyViolationCodes != null || primaryKeyViolationSqlStates != null) {
            SQLException sqlEx = findSQLException(ex);
            if (sqlEx != null) {
                if (primaryKeyViolationCodes != null) {
                    int errorCode = sqlEx.getErrorCode();
                    for (int primaryKeyViolationCode : primaryKeyViolationCodes) {
                        if (primaryKeyViolationCode == errorCode) {
                            primaryKeyViolation = true;
                            break;
                        }
                    }
                }

                if (primaryKeyViolationSqlStates != null) {
                    String sqlState = sqlEx.getSQLState();
                    if (sqlState != null) {
                        for (String primaryKeyViolationSqlState : primaryKeyViolationSqlStates) {
                            if (primaryKeyViolationSqlState != null
                                    && primaryKeyViolationSqlState.equals(sqlState)) {
                                primaryKeyViolation = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return primaryKeyViolation;
    }

    protected SQLException findSQLException(Throwable ex) {
        if (ex instanceof SQLException) {
            return (SQLException) ex;
        } else {
            Throwable cause = ex.getCause();
            if (cause != null && !cause.equals(ex)) {
                return findSQLException(cause);
            }
        }
        return null;
    }

}
