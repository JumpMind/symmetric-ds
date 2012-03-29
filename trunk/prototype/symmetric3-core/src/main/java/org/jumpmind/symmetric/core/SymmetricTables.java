package org.jumpmind.symmetric.core;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;

public class SymmetricTables extends Database {

    private static final long serialVersionUID = 1L;
    
    public static final String REGISTRATION_REDIRECT = "REGISTRATION_REDIRECT";

    public static final String PARAMETER = "PARAMETER";

    public static final String TRIGGER_ROUTER = "TRIGGER_ROUTER";

    public static final String ROUTER = "ROUTER";

    public static final String TRIGGER = "TRIGGER";

    public static final String NODE_GROUP_CHANNEL_WINDOW = "NODE_GROUP_CHANNEL_WINDOW";

    public static final String NODE_CHANNEL_CTL = "NODE_CHANNEL_CTL";

    public static final String CHANNEL = "CHANNEL";

    public static final String NODE_HOST_JOB_STATS = "NODE_HOST_JOB_STATS";

    public static final String NODE_HOST_STATS = "NODE_HOST_STATS";

    public static final String NODE_HOST_CHANNEL_STATS = "NODE_HOST_CHANNEL_STATS";

    public static final String NODE_HOST = "NODE_HOST";

    public static final String NODE_GROUP_LINK = "NODE_GROUP_LINK";

    public static final String NODE_GROUP = "NODE_GROUP";

    public static final String NODE_IDENTITY = "NODE_IDENTITY";

    public static final String NODE_SECURITY = "NODE_SECURITY";

    public static final String NODE = "NODE";
    
    public static final String DATA_GAP = "DATA_GAP";
    
    public static final String DATA_EVENT = "DATA_EVENT";
    
    public static final String TRIGGER_HIST = "TRIGGER_HIST";
    
    public static final String LOCK = "LOCK";
    
    public static final String INCOMING_BATCH = "INCOMING_BATCH";
    
    public static final String OUTGOING_BATCH = "OUTGOING_BATCH";
    
    public static final String REGISTRATION_REQUEST = "REGISTRATION_REQUEST";
    
    public static final String DATA = "DATA";

    public static final String DEFAULT_PREFIX = "SYM";

    protected String prefix = DEFAULT_PREFIX;
    
    public static String[] DROP_ORDER = {
        DATA_GAP,
        NODE_CHANNEL_CTL,
        NODE_GROUP_CHANNEL_WINDOW,
        DATA_EVENT,
        TRIGGER_HIST,
        TRIGGER,
        ROUTER,
        TRIGGER_ROUTER,
        NODE_SECURITY,
        NODE_IDENTITY,
        LOCK,
        NODE_HOST,
        NODE,
        NODE_GROUP_LINK,
        NODE_GROUP,
        INCOMING_BATCH,
        CHANNEL,
        OUTGOING_BATCH,
        PARAMETER,
        NODE_HOST_CHANNEL_STATS,
        NODE_HOST_STATS,
        NODE_HOST_JOB_STATS,
        REGISTRATION_REDIRECT,
        REGISTRATION_REQUEST,
        DATA
    };

    protected Map<String, Table> byName = new HashMap<String, Table>();

    public SymmetricTables() {
        this(DEFAULT_PREFIX);
    }

    public SymmetricTables(String prefix) {
        this.prefix = prefix;
        addTable(buildNodeTable());
        addTable(buildNodeSecurity());
        addTable(buildNodeIdentity());
        addTable(buildParameter());
        addTable(buildChannel());
        addTable(buildNodeChannelCtl());
        addTable(buildNodeGroup());
        addTable(buildNodeGroupLink());
        addTable(buildNodeHost());
        addTable(buildNodeHostChannelStats());
        addTable(buildNodeHostJobStats());
        addTable(buildNodeHostStats());
        addTable(buildNodeGroupChannelWindow());
        addTable(buildRegistrationRedirect());
        addTable(buildRouter());
        addTable(buildTrigger());
        addTable(buildTriggerRouter());
    }

    public Table getSymmetricTable(String suffix) {
        return byName.get(prependPrefix(suffix));
    }
    
    public Table[] getSymmetricTables(String... suffixs) {
        Table[] tables = new Table[suffixs.length];
        int index = 0;
        for (String suffix : suffixs) {
            tables[index++] = getSymmetricTable(suffix);
        }
        return tables;
    }

    @Override
    public void addTable(Table table) {
        this.byName.put(table.getTableName(), table);
        super.addTable(table);
    }

    protected String prependPrefix(String suffix) {
        if (StringUtils.isNotBlank(prefix)) {
            return String.format("%s_%s", prefix, suffix);
        } else {
            return suffix;
        }
    }

    protected Table buildNodeTable() {
        Table table = new Table(prependPrefix(NODE));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("EXTERNAL_ID", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("SYNC_ENABLED", TypeMap.BOOLEAN, "1", false, true, false, "0"));
        table.addColumn(new Column("SYNC_URL", TypeMap.VARCHAR, "255", false, false, false));
        table.addColumn(new Column("SCHEMA_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("SYMMETRIC_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("DATABASE_TYPE", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("DATABASE_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("HEARTBEAT_TIME", TypeMap.TIMESTAMP, "50", false, false, false));
        table.addColumn(new Column("TIMEZONE_OFFSET", TypeMap.VARCHAR, "6", false, false, false));
        table.addColumn(new Column("BATCH_TO_SEND_COUNT", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("BATCH_IN_ERROR_COUNT", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("CREATED_AT_NODE_ID", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("DEPLOYMENT_TYPE", TypeMap.VARCHAR, "50", false, false, false));
        return table;
    }

    protected Table buildNodeSecurity() {
        Table table = new Table(prependPrefix(NODE_SECURITY));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("NODE_PASSWORD", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("REGISTRATION_ENABLED", TypeMap.BOOLEAN, "1", false, true,
                false, "0"));
        table.addColumn(new Column("REGISTRATION_TIME", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("INITIAL_LOAD_ENABLED", TypeMap.BOOLEAN, "1", false, true,
                false, "0"));
        table.addColumn(new Column("INITIAL_LOAD_TIME", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("CREATED_AT_NODE_ID", TypeMap.VARCHAR, "50", false, false, false));
        return table;
    }

    protected Table buildNodeIdentity() {
        Table table = new Table(prependPrefix(NODE_IDENTITY));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        return table;
    }

    protected Table buildNodeGroup() {
        Table table = new Table(prependPrefix(NODE_GROUP));
        table.addColumn(new Column("NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("DESCRIPTION", TypeMap.VARCHAR, "255", false, false, false));
        return table;
    }

    protected Table buildNodeGroupLink() {
        Table table = new Table(prependPrefix(NODE_GROUP_LINK));
        table.addColumn(new Column("SOURCE_NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("TARGET_NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("DATA_EVENT_ACTION", TypeMap.VARCHAR, "1", false, true, false));
        return table;
    }

    protected Table buildNodeHost() {
        Table table = new Table(prependPrefix(NODE_HOST));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("HOST_NAME", TypeMap.VARCHAR, "60", false, true, true));
        table.addColumn(new Column("IP_ADDRESS", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("OS_USER", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("OS_NAME", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("OS_ARCH", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("OS_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("AVAILABLE_PROCESSORS", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("FREE_MEMORY_BYTES", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("TOTAL_MEMORY_BYTES", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("MAX_MEMORY_BYTES", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("JAVA_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("JAVA_VENDOR", TypeMap.VARCHAR, "255", false, false, false));
        table.addColumn(new Column("SYMMETRIC_VERSION", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("TIMEZONE_OFFSET", TypeMap.VARCHAR, "6", false, false, false));
        table.addColumn(new Column("HEARTBEAT_TIME", TypeMap.TIMESTAMP, "50", false, false, false));
        table.addColumn(new Column("LAST_RESTART_TIME", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("CREATE_TIME", TypeMap.TIMESTAMP, "50", false, false, false));
        return table;
    }

    protected Table buildNodeHostChannelStats() {
        Table table = new Table(prependPrefix(NODE_HOST_CHANNEL_STATS));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("HOST_NAME", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("CHANNEL_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("START_TIME", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("END_TIME", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("DATA_ROUTED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("DATA_UNROUTED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("DATA_EVENT_INSERTED", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("DATA_EXTRACTED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("DATA_BYTES_EXTRACTED", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("DATA_EXTRACTED_ERRORS", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("DATA_BYTES_SENT", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("DATA_SENT", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("DATA_SENT_ERRORS", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("DATA_LOADED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("DATA_BYTES_LOADED", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("DATA_LOADED_ERRORS", TypeMap.BIGINT, null, false, false, false,
                "0"));
        // <index name="IDX_ND_HST_CHNL_STS">
        // <index-column name="NODE_ID"/>
        // <index-column name="START_TIME"/>
        // <index-column name="END_TIME"/>
        return table;
    }

    protected Table buildNodeHostStats() {
        Table table = new Table(prependPrefix(NODE_HOST_STATS));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("HOST_NAME", TypeMap.VARCHAR, "60", false, true, true));
        table.addColumn(new Column("START_TIME", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("END_TIME", TypeMap.TIMESTAMP, null, false, true, true));
        table.addColumn(new Column("RESTARTED", TypeMap.BIGINT, null, false, true, false, "0"));
        table.addColumn(new Column("NODES_PULLED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("TOTAL_NODES_PULL_TIME", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("NODES_PUSHED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("TOTAL_NODES_PUSH_TIME", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("NODES_REJECTED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("NODES_REGISTERED", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("NODES_LOADED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("NODES_DISABLED", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("PURGED_DATA_ROWS", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("PURGED_DATA_EVENT_ROWS", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("PURGED_BATCH_OUTGOING_ROWS", TypeMap.BIGINT, null, false,
                false, false, "0"));
        table.addColumn(new Column("PURGED_BATCH_INCOMING_ROWS", TypeMap.BIGINT, null, false,
                false, false, "0"));
        table.addColumn(new Column("TRIGGERS_CREATED_COUNT", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("TRIGGERS_REBUILT_COUNT", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("TRIGGERS_REMOVED_COUNT", TypeMap.BIGINT, null, false, false,
                false, "0"));
        // <index name="IDX_ND_HST_STS">
        // <index-column name="NODE_ID"/>
        // <index-column name="START_TIME"/>
        // <index-column name="END_TIME"/>
        // </index>
        return table;
    }

    protected Table buildNodeHostJobStats() {
        Table table = new Table(prependPrefix(NODE_HOST_JOB_STATS));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("HOST_NAME", TypeMap.VARCHAR, "60", false, true, true));
        table.addColumn(new Column("JOB_NAME", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("START_TIME", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("END_TIME", TypeMap.TIMESTAMP, null, false, true, true));
        table.addColumn(new Column("PROCESSED_COUNT", TypeMap.BIGINT, null, false, false, false,
                "0"));
        // <index name="IDX_ND_HST_JOB">
        // <index-column name="JOB_NAME"/>
        // </index>
        return table;
    }

    protected Table buildChannel() {
        Table table = new Table(prependPrefix(CHANNEL));
        table.addColumn(new Column("CHANNEL_ID", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("PROCESSING_ORDER", TypeMap.INTEGER, null, false, true, false,
                "1"));
        table.addColumn(new Column("MAX_BATCH_SIZE", TypeMap.INTEGER, null, false, true, false,
                "1000"));
        table.addColumn(new Column("MAX_BATCH_TO_SEND", TypeMap.INTEGER, null, false, true, false,
                "60"));
        table.addColumn(new Column("MAX_DATA_TO_ROUTE", TypeMap.INTEGER, null, false, true, false,
                "100000"));
        table.addColumn(new Column("EXTRACT_PERIOD_MILLIS", TypeMap.INTEGER, null, false, true,
                false, "0"));
        table.addColumn(new Column("ENABLED", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("USE_OLD_DATA_TO_ROUTE", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("USE_ROW_DATA_TO_ROUTE", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("USE_PK_DATA_TO_ROUTE", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("CONTAINS_BIG_LOB", TypeMap.BOOLEAN, "1", false, true, false,
                "0"));
        table.addColumn(new Column("BATCH_ALGORITHM", TypeMap.VARCHAR, "50", false, true, false,
                "default"));
        table.addColumn(new Column("DESCRIPTION", TypeMap.VARCHAR, "255", false, false, false));
        return table;
    }

    protected Table buildNodeChannelCtl() {
        Table table = new Table(prependPrefix(NODE_CHANNEL_CTL));
        table.addColumn(new Column("NODE_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("CHANNEL_ID", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("SUSPEND_ENABLED", TypeMap.BOOLEAN, "1", false, false, false,
                "0"));
        table.addColumn(new Column("IGNORE_ENABLED", TypeMap.BOOLEAN, "1", false, false, false, "0"));
        table.addColumn(new Column("LAST_EXTRACT_TIME", TypeMap.TIMESTAMP, "50", false, false,
                false));
        return table;
    }

    protected Table buildNodeGroupChannelWindow() {
        Table table = new Table(prependPrefix(NODE_GROUP_CHANNEL_WINDOW));
        table.addColumn(new Column("NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("CHANNEL_ID", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("START_TIME", TypeMap.TIME, "50", false, true, true));
        table.addColumn(new Column("END_TIME", TypeMap.TIME, "50", false, true, true));
        table.addColumn(new Column("ENABLED", TypeMap.BOOLEAN, "1", false, true, false, "0"));
        return table;
    }

    protected Table buildTrigger() {
        Table table = new Table(prependPrefix(TRIGGER));
        table.addColumn(new Column("TRIGGER_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("SOURCE_CATALOG_NAME", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("SOURCE_SCHEMA_NAME", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("SOURCE_TABLE_NAME", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("CHANNEL_ID", TypeMap.VARCHAR, "20", false, true, false));
        table.addColumn(new Column("SYNC_ON_UPDATE", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("SYNC_ON_INSERT", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("SYNC_ON_DELETE", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("SYNC_ON_INCOMING_BATCH", TypeMap.BOOLEAN, "1", false, true,
                false));
        table.addColumn(new Column("NAME_FOR_UPDATE_TRIGGER", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("NAME_FOR_INSERT_TRIGGER", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("NAME_FOR_DELETE_TRIGGER", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("SYNC_ON_UPDATE_CONDITION", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("SYNC_ON_INSERT_CONDITION", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("SYNC_ON_DELETE_CONDITION", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("EXTERNAL_SELECT", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("TX_ID_EXPRESSION", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("EXCLUDED_COLUMN_NAMES", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("CREATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false));
        table.addColumn(new Column("LAST_UPDATE_BY", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("LAST_UPDATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildRouter() {
        Table table = new Table(prependPrefix(ROUTER));
        table.addColumn(new Column("ROUTER_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("TARGET_CATALOG_NAME", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("TARGET_SCHEMA_NAME", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("TARGET_TABLE_NAME", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("SOURCE_NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("TARGET_NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("ROUTER_TYPE", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("ROUTER_EXPRESSION", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("SYNC_ON_UPDATE", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("SYNC_ON_INSERT", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("SYNC_ON_DELETE", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("CREATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false, "1"));
        table.addColumn(new Column("LAST_UPDATE_BY", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("LAST_UPDATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildTriggerRouter() {
        Table table = new Table(prependPrefix(TRIGGER_ROUTER));
        table.addColumn(new Column("TRIGGER_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("ROUTER_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("INITIAL_LOAD_ORDER", TypeMap.INTEGER, null, false, true, false,
                "1"));
        table.addColumn(new Column("INITIAL_LOAD_SELECT", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("PING_BACK_ENABLED", TypeMap.BOOLEAN, "1", false, true, false,
                "0"));
        table.addColumn(new Column("CREATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false));
        table.addColumn(new Column("LAST_UPDATE_BY", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("LAST_UPDATE_TIME", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildParameter() {
        Table table = new Table(prependPrefix(PARAMETER));
        table.addColumn(new Column("EXTERNAL_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("NODE_GROUP_ID", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("PARAM_KEY", TypeMap.VARCHAR, "80", false, true, true));
        table.addColumn(new Column("PARAM_VALUE", TypeMap.LONGVARCHAR, null, false, false, false));
        return table;
    }

    protected Table buildRegistrationRedirect() {
        Table table = new Table(prependPrefix(REGISTRATION_REDIRECT));
        table.addColumn(new Column("REGISTRANT_EXTERNAL_ID", TypeMap.VARCHAR, "50", false, true,
                true));
        table.addColumn(new Column("REGISTRATION_NODE_ID", TypeMap.VARCHAR, "50", false, true,
                false));
        return table;
    }

}
