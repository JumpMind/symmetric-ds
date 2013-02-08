package org.jumpmind.symmetric.route;

import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

public class AuditTableDataRouter extends AbstractDataRouter {

    private static final String COLUMN_AUDIT_EVENT = "AUDIT_EVENT";

    private static final String COLUMN_AUDIT_TIME = "AUDIT_TIME";

    private static final String COLUMN_AUDIT_ID = "AUDIT_ID";

    private ISymmetricEngine engine;

    private Map<String, Table> auditTables = new HashMap<String, Table>();

    public AuditTableDataRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        DataEventType eventType = dataMetaData.getData().getDataEventType();
        if (eventType == DataEventType.INSERT || eventType == DataEventType.UPDATE
                || eventType == DataEventType.DELETE) {
            IParameterService parameterService = engine.getParameterService();
            IDatabasePlatform platform = engine.getDatabasePlatform();
            TriggerHistory triggerHistory = dataMetaData.getTriggerHistory();
            Table table = dataMetaData.getTable().copyAndFilterColumns(
                    triggerHistory.getParsedColumnNames(), triggerHistory.getParsedPkColumnNames(),
                    true);
            String tableName = table.getFullyQualifiedTableName();
            Table auditTable = auditTables.get(tableName);
            if (auditTable == null) {
                auditTable = toAuditTable(table);
                auditTables.put(tableName, auditTable);
                if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE)) {
                    platform.alterTables(true, auditTable);
                }
            }
            String auditTableName = auditTable.getFullyQualifiedTableName(platform
                    .getDatabaseInfo().getDelimiterToken());

            ISqlTemplate template = platform.getSqlTemplate();
            Map<String, Object> values = new HashMap<String, Object>(getNewDataAsObject(null,
                    dataMetaData, engine.getSymmetricDialect()));
            Long sequence = (Long) context.get(auditTableName);
            if (sequence == null) {
                sequence = 1l + template.queryForLong(String.format("select max(%s) from %s",
                        COLUMN_AUDIT_ID, auditTableName));
            } else {
                sequence = 1l + sequence;
            }
            context.put(auditTableName, sequence);
            values.put(COLUMN_AUDIT_ID, sequence);
            values.put(COLUMN_AUDIT_TIME, new Date());
            values.put(COLUMN_AUDIT_EVENT, eventType.getCode());
            DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, auditTable);
            int[] types = statement.getTypes();
            Object[] args = statement.getValueArray(values);
            String sql = statement.getSql();
            template.update(sql, args, types);
        }
        return null;
    }

    protected Table toAuditTable(Table table) {
        Table auditTable = table.copy();
        String tableName = table.getName();
        auditTable.setName(String.format("%s_AUDIT", tableName));
        Column[] columns = auditTable.getColumns();
        auditTable.removeAllColumns();
        auditTable.addColumn(new Column(COLUMN_AUDIT_ID, true, Types.BIGINT, 0, 0));
        auditTable.addColumn(new Column(COLUMN_AUDIT_TIME, false, Types.TIMESTAMP, 0, 0));
        auditTable.addColumn(new Column(COLUMN_AUDIT_EVENT, false, Types.CHAR, 1, 0));
        for (Column column : columns) {
            column.setRequired(false);
            column.setPrimaryKey(false);
            auditTable.addColumn(column);
        }
        auditTable.removeAllForeignKeys();
        auditTable.removeAllIndices();
        engine.getDatabasePlatform().alterCaseToMatchDatabaseDefaultCase(auditTable);
        return auditTable;
    }

}
