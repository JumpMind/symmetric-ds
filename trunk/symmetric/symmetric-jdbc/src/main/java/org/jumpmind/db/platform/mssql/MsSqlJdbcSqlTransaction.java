package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;

public class MsSqlJdbcSqlTransaction extends JdbcSqlTransaction {

    public MsSqlJdbcSqlTransaction(JdbcSqlTemplate sqltemplate) {
        super(sqltemplate);
    }

    @Override
    public void allowInsertIntoAutoIncrementColumns(boolean allow, Table table) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            if (allow) {
                prepareAndExecute(String.format("SET IDENTITY_INSERT %s ON",
                        table.getFullyQualifiedTableName()));
            } else {
                prepareAndExecute(String.format("SET IDENTITY_INSERT %s OFF",
                        table.getFullyQualifiedTableName()));
            }
        }
    }

}
