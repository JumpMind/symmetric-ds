package org.jumpmind.db.platform;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.oracle.OracleDmlStatement;
import org.jumpmind.db.platform.postgresql.PostgreSqlDmlStatement;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;

final public class DmlStatementFactory {

    private DmlStatementFactory() {
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType, Table table, boolean useQuotedIdentifiers) {
        return createDmlStatement(databaseName, dmlType, table.getCatalog(), table.getSchema(),
                table.getName(), table.getPrimaryKeyColumns(), table.getColumns(), null, useQuotedIdentifiers);
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType,
            String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, boolean[] nullKeyValues, boolean useQuotedIdentifiers) {
        IDdlBuilder ddlBuilder = DdlBuilderFactory.createDdlBuilder(databaseName);
        if (DatabaseNamesConstants.ORACLE.equals(databaseName)) {
            return new OracleDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, ddlBuilder.getDatabaseInfo().isDateOverridesToTimestamp(), useQuotedIdentifiers ? ddlBuilder
                            .getDatabaseInfo().getDelimiterToken() : "", nullKeyValues);
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(databaseName)) {
            return new PostgreSqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, ddlBuilder.getDatabaseInfo().isDateOverridesToTimestamp(), useQuotedIdentifiers  ? ddlBuilder
                            .getDatabaseInfo().getDelimiterToken() : "", nullKeyValues);
        } else {
            return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    ddlBuilder.getDatabaseInfo().isDateOverridesToTimestamp(), useQuotedIdentifiers ? ddlBuilder
                            .getDatabaseInfo().getDelimiterToken() : "", nullKeyValues);
        }

    }

}
