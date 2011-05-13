package org.jumpmind.symmetric.core.process.sql;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.sql.StatementBuilder;

public class SqlDataContext extends DataContext {

    protected Map<String, Map<DataEventType, StatementBuilder>> statements = new HashMap<String, Map<DataEventType, StatementBuilder>>();

    public StatementBuilder getStatementBuilder(Table table, Data data) {
        StatementBuilder builder = null;
        Map<DataEventType, StatementBuilder> byDmlForTable = statements.get(table
                .getFullyQualifiedTableName());
        if (byDmlForTable != null) {
            builder = byDmlForTable.get(data.getEventType());
        }
        return builder;
    }

    public void putStatementBuilder(Table table, Data data, StatementBuilder builder) {
        String tableName = table.getFullyQualifiedTableName();
        Map<DataEventType, StatementBuilder> byDmlForTable = statements.get(tableName);
        if (byDmlForTable == null) {
            byDmlForTable = new HashMap<DataEventType, StatementBuilder>();
            statements.put(tableName, byDmlForTable);
        }
        byDmlForTable.put(data.getEventType(), builder);
    }
}
