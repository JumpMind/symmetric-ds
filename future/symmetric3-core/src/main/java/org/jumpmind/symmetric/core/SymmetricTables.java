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
    
    public static final String REGISTRATION_REDIRECT = "registration_redirect";

    public static final String PARAMETER = "parameter";

    public static final String TRIGGER_ROUTER = "trigger_router";

    public static final String ROUTER = "router";

    public static final String TRIGGER = "trigger";

    public static final String NODE_GROUP_CHANNEL_WINDOW = "node_group_channel_window";

    public static final String NODE_CHANNEL_CTL = "node_channel_ctl";

    public static final String CHANNEL = "channel";

    public static final String NODE_HOST_JOB_STATS = "node_host_job_stats";

    public static final String NODE_HOST_STATS = "node_host_stats";

    public static final String NODE_HOST_CHANNEL_STATS = "node_host_channel_stats";

    public static final String NODE_HOST = "node_host";

    public static final String NODE_GROUP_LINK = "node_group_link";

    public static final String NODE_GROUP = "node_group";

    public static final String NODE_IDENTITY = "node_identity";

    public static final String NODE_SECURITY = "node_security";

    public static final String NODE = "node";

    public static final String DEFAULT_PREFIX = "sym";

    protected String prefix = DEFAULT_PREFIX;

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

    public Table getSymmetricTable(String name) {
        return byName.get(name);
    }

    @Override
    public void addTable(Table table) {
        this.byName.put(table.getTableName().substring(this.prefix.length()), table);
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

    protected Table buildNodeSecurity() {
        Table table = new Table(prependPrefix(NODE_SECURITY));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("node_password", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("registration_enabled", TypeMap.BOOLEAN, "1", false, true,
                false, "0"));
        table.addColumn(new Column("registration_time", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("initial_load_enabled", TypeMap.BOOLEAN, "1", false, true,
                false, "0"));
        table.addColumn(new Column("initial_load_time", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("created_at_node_id", TypeMap.VARCHAR, "50", false, false, false));
        return table;
    }

    protected Table buildNodeIdentity() {
        Table table = new Table(prependPrefix(NODE_IDENTITY));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        return table;
    }

    protected Table buildNodeGroup() {
        Table table = new Table(prependPrefix(NODE_GROUP));
        table.addColumn(new Column("node_group_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("description", TypeMap.VARCHAR, "255", false, false, false));
        return table;
    }

    protected Table buildNodeGroupLink() {
        Table table = new Table(prependPrefix(NODE_GROUP_LINK));
        table.addColumn(new Column("source_node_group_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("target_node_group_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("data_event_action", TypeMap.VARCHAR, "1", false, true, false));
        return table;
    }

    protected Table buildNodeHost() {
        Table table = new Table(prependPrefix(NODE_HOST));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("host_name", TypeMap.VARCHAR, "60", false, false, true));
        table.addColumn(new Column("ip_address", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("os_user", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("os_name", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("os_arch", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("os_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("available_processors", TypeMap.INTEGER, null, false, false,
                false, "0"));
        table.addColumn(new Column("free_memory_bytes", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("total_memory_bytes", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("max_memory_bytes", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("java_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("java_vendor", TypeMap.VARCHAR, "255", false, false, false));
        table.addColumn(new Column("symmetric_version", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("timezone_offset", TypeMap.VARCHAR, "6", false, false, false));
        table.addColumn(new Column("heartbeat_time", TypeMap.TIMESTAMP, "50", false, false, false));
        table.addColumn(new Column("last_restart_time", TypeMap.TIMESTAMP, "50", false, false,
                false));
        table.addColumn(new Column("create_time", TypeMap.TIMESTAMP, "50", false, false, false));
        return table;
    }

    protected Table buildNodeHostChannelStats() {
        Table table = new Table(prependPrefix(NODE_HOST_CHANNEL_STATS));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("host_name", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("channel_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("start_time", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("end_time", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("data_routed", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("data_unrouted", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("data_event_inserted", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("data_extracted", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("data_bytes_extracted", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("data_extracted_errors", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("data_bytes_sent", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("data_sent", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("data_sent_errors", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("data_loaded", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("data_bytes_loaded", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("data_loaded_errors", TypeMap.BIGINT, null, false, false, false,
                "0"));
        // <index name="idx_nd_hst_chnl_sts">
        // <index-column name="node_id"/>
        // <index-column name="start_time"/>
        // <index-column name="end_time"/>
        return table;
    }

    protected Table buildNodeHostStats() {
        Table table = new Table(prependPrefix(NODE_HOST_STATS));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("host_name", TypeMap.VARCHAR, "60", false, true, true));
        table.addColumn(new Column("start_time", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("end_time", TypeMap.TIMESTAMP, null, false, true, true));
        table.addColumn(new Column("restarted", TypeMap.BIGINT, null, false, true, false, "0"));
        table.addColumn(new Column("nodes_pulled", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("total_nodes_pull_time", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("nodes_pushed", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("total_nodes_push_time", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("nodes_rejected", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("nodes_registered", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("nodes_loaded", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("nodes_disabled", TypeMap.BIGINT, null, false, false, false, "0"));
        table.addColumn(new Column("purged_data_rows", TypeMap.BIGINT, null, false, false, false,
                "0"));
        table.addColumn(new Column("purged_data_event_rows", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("purged_batch_outgoing_rows", TypeMap.BIGINT, null, false,
                false, false, "0"));
        table.addColumn(new Column("purged_batch_incoming_rows", TypeMap.BIGINT, null, false,
                false, false, "0"));
        table.addColumn(new Column("triggers_created_count", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("triggers_rebuilt_count", TypeMap.BIGINT, null, false, false,
                false, "0"));
        table.addColumn(new Column("triggers_removed_count", TypeMap.BIGINT, null, false, false,
                false, "0"));
        // <index name="idx_nd_hst_sts">
        // <index-column name="node_id"/>
        // <index-column name="start_time"/>
        // <index-column name="end_time"/>
        // </index>
        return table;
    }

    protected Table buildNodeHostJobStats() {
        Table table = new Table(prependPrefix(NODE_HOST_JOB_STATS));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("host_name", TypeMap.VARCHAR, "60", false, true, true));
        table.addColumn(new Column("job_name", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("start_time", TypeMap.TIMESTAMP, "50", false, true, true));
        table.addColumn(new Column("end_time", TypeMap.TIMESTAMP, null, false, true, true));
        table.addColumn(new Column("processed_count", TypeMap.BIGINT, null, false, false, false,
                "0"));
        // <index name="idx_nd_hst_job">
        // <index-column name="job_name"/>
        // </index>
        return table;
    }

    protected Table buildChannel() {
        Table table = new Table(prependPrefix(CHANNEL));
        table.addColumn(new Column("channel_id", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("processing_order", TypeMap.INTEGER, null, false, true, false,
                "1"));
        table.addColumn(new Column("max_batch_size", TypeMap.INTEGER, null, false, true, false,
                "1000"));
        table.addColumn(new Column("max_batch_to_send", TypeMap.INTEGER, null, false, true, false,
                "60"));
        table.addColumn(new Column("max_data_to_route", TypeMap.INTEGER, null, false, true, false,
                "100000"));
        table.addColumn(new Column("extract_period_millis", TypeMap.INTEGER, null, false, true,
                false, "0"));
        table.addColumn(new Column("enabled", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("use_old_data_to_route", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("use_row_data_to_route", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("use_pk_data_to_route", TypeMap.BOOLEAN, "1", false, true,
                false, "1"));
        table.addColumn(new Column("contains_big_lob", TypeMap.BOOLEAN, "1", false, true, false,
                "0"));
        table.addColumn(new Column("batch_algorithm", TypeMap.VARCHAR, "50", false, true, false,
                "default"));
        table.addColumn(new Column("description", TypeMap.VARCHAR, "255", false, false, false));
        return table;
    }

    protected Table buildNodeChannelCtl() {
        Table table = new Table(prependPrefix(NODE_CHANNEL_CTL));
        table.addColumn(new Column("node_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("channel_id", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("suspend_enabled", TypeMap.BOOLEAN, "1", false, false, false,
                "0"));
        table.addColumn(new Column("ignore_enabled", TypeMap.BOOLEAN, "1", false, false, false, "0"));
        table.addColumn(new Column("last_extract_time", TypeMap.TIMESTAMP, "50", false, false,
                false));
        return table;
    }

    protected Table buildNodeGroupChannelWindow() {
        Table table = new Table(prependPrefix(NODE_GROUP_CHANNEL_WINDOW));
        table.addColumn(new Column("node_group_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("channel_id", TypeMap.VARCHAR, "20", false, true, true));
        table.addColumn(new Column("start_time", TypeMap.TIME, "50", false, true, true));
        table.addColumn(new Column("end_time", TypeMap.TIME, "50", false, true, true));
        table.addColumn(new Column("enabled", TypeMap.BOOLEAN, "1", false, true, false, "0"));
        return table;
    }

    protected Table buildTrigger() {
        Table table = new Table(prependPrefix(TRIGGER));
        table.addColumn(new Column("trigger_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("source_catalog_name", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("source_schema_name", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("source_table_name", TypeMap.VARCHAR, "50", false, true, false));
        table.addColumn(new Column("channel_id", TypeMap.VARCHAR, "20", false, true, false));
        table.addColumn(new Column("sync_on_update", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("sync_on_insert", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("sync_on_delete", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("sync_on_incoming_batch", TypeMap.BOOLEAN, "1", false, true,
                false));
        table.addColumn(new Column("name_for_update_trigger", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("name_for_insert_trigger", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("name_for_delete_trigger", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("sync_on_update_condition", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("sync_on_insert_condition", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("sync_on_delete_condition", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("external_select", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("tx_id_expression", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("excluded_column_names", TypeMap.LONGVARCHAR, null, false,
                false, false));
        table.addColumn(new Column("create_time", TypeMap.TIMESTAMP, "50", false, true, false));
        table.addColumn(new Column("last_update_by", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("last_update_time", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildRouter() {
        Table table = new Table(prependPrefix(ROUTER));
        table.addColumn(new Column("router_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("target_catalog_name", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("target_schema_name", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("target_table_name", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("source_node_group_id", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("target_node_group_id", TypeMap.VARCHAR, "50", false, false,
                false));
        table.addColumn(new Column("router_type", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("router_expression", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("sync_on_update", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("sync_on_insert", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("sync_on_delete", TypeMap.BOOLEAN, "1", false, true, false, "1"));
        table.addColumn(new Column("create_time", TypeMap.TIMESTAMP, "50", false, true, false, "1"));
        table.addColumn(new Column("last_update_by", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("last_update_time", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildTriggerRouter() {
        Table table = new Table(prependPrefix(TRIGGER_ROUTER));
        table.addColumn(new Column("trigger_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("router_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("initial_load_order", TypeMap.INTEGER, null, false, true, false,
                "1"));
        table.addColumn(new Column("initial_load_select", TypeMap.LONGVARCHAR, null, false, false,
                false));
        table.addColumn(new Column("ping_back_enabled", TypeMap.BOOLEAN, "1", false, true, false,
                "0"));
        table.addColumn(new Column("create_time", TypeMap.TIMESTAMP, "50", false, true, false));
        table.addColumn(new Column("last_update_by", TypeMap.VARCHAR, "50", false, false, false));
        table.addColumn(new Column("last_update_time", TypeMap.TIMESTAMP, "50", false, true, false));
        return table;
    }

    protected Table buildParameter() {
        Table table = new Table(prependPrefix(PARAMETER));
        table.addColumn(new Column("external_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("node_group_id", TypeMap.VARCHAR, "50", false, true, true));
        table.addColumn(new Column("param_key", TypeMap.VARCHAR, "80", false, true, true));
        table.addColumn(new Column("param_value", TypeMap.LONGVARCHAR, null, false, false, false));
        return table;
    }

    protected Table buildRegistrationRedirect() {
        Table table = new Table(prependPrefix(REGISTRATION_REDIRECT));
        table.addColumn(new Column("registrant_external_id", TypeMap.VARCHAR, "50", false, true,
                true));
        table.addColumn(new Column("registration_node_id", TypeMap.VARCHAR, "50", false, true,
                false));
        return table;
    }

}
