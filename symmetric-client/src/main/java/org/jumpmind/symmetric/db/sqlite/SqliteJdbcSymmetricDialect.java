package org.jumpmind.symmetric.db.sqlite;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.service.IParameterService;
import org.sqlite.Function;
import org.sqlite.SQLiteConnection;

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
                @SuppressWarnings("rawtypes")
				SQLiteConnection unwrapped = ((SQLiteConnection)((DelegatingConnection)con).getInnermostDelegate());
                
                Function.create(unwrapped, name, new Function() {
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
