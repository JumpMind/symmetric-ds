package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;

public class SymmetricDatabase extends Database {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_PREFIX = "sym";
    
    protected String prefix = DEFAULT_PREFIX;

    public SymmetricDatabase() {
        this(DEFAULT_PREFIX);
    }

    public SymmetricDatabase(String prefix) {
        this.prefix = prefix;
        addTable(buildNodeTable());
    }

    protected String prependPrefix(String suffix) {
        if (StringUtils.isNotBlank(prefix)) {
            return String.format("%s_%s", prefix, suffix);
        } else {
            return suffix;
        }
    }

    protected Table buildNodeTable() {
        Table table = new Table(prependPrefix("node"));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("node_group_id", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("external_id", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("sync_enabled", TypeMap.BOOLEAN, "1", false, true, false, "0"));
        table.addColumn(new Column("sync_url", TypeMap.VARCHAR, "255", false, false, false));
        table.addColumn(new Column("schema_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("symmetric_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("database_type", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("database_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("heartbeat_time", TypeMap.TIMESTAMP, "50", false, false, false));
        table.addColumn(new Column("timezone_offset", TypeMap.VARCHAR, "6", false, false, false));
        table.addColumn(new Column("batch_to_send_count", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("batch_in_error_count", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("created_at_node_id", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("deployment_type", TypeMap.VARCHAR, "50", false, false, false));
        return table;
    }
}
