package org.jumpmind.symmetric.db.sqlite;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.service.IParameterService;

public class SqliteJdbcSymmetricDialect extends SqliteSymmetricDialect {

    public SqliteJdbcSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
    }

    @Override
    protected void setSqliteFunctionResult(ISqlTransaction transaction, final String name, final String result){
        JdbcSqlTransaction trans = (JdbcSqlTransaction)transaction;
        trans.executeCallback(new IConnectionCallback<Object>() {
            @Override
            public Object execute(Connection con) throws SQLException {
                org.sqlite.SQLiteConnection unwrapped = ((org.sqlite.SQLiteConnection)((org.apache.commons.dbcp.DelegatingConnection)con).getInnermostDelegate());
                
                org.sqlite.Function.create(unwrapped, name, new org.sqlite.Function() {
                    @Override
                    protected void xFunc() throws SQLException {
                        this.result(result);
                    }
                });
                
                return null;
            }
        });
    }

}
