package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class FileSyncServiceSqlMap extends AbstractSqlMap {

    public FileSyncServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        
        putSql("selectFileTriggersSql", 
                " select distinct t.trigger_id as trigger_id, base_dir, recursive,              " +
                "        includes_files, excludes_files,                                        " +
        		"        sync_on_create, sync_on_modified, t.sync_on_delete as sync_on_delete,  " +
                "        t.before_copy_script as before_copy_script, " +
                "        t.after_copy_script as after_copy_script, " +
        		"        t.create_time as create_time, t.last_update_by as last_update_by,      " +
        		"        t.last_update_time as last_update_time                                 " +
        		" from $(file_trigger) t                                                        ");
        
        putSql("triggerIdWhere", "where trigger_id=?");
       
        
        putSql("updateFileTriggerSql",                 
                " update $(file_trigger) set base_dir=?, recursive=?, includes_files=?,         " +
        		"  excludes_files=?, sync_on_create=?, sync_on_modified=?, sync_on_delete=?,    " +
                "  before_copy_script=?, after_copy_script=?,                                   " +
        		"  last_update_by=?, last_update_time=? where trigger_id=?                      ");

        putSql("insertFileTriggerSql",                 
                " insert into $(file_trigger) (base_dir, recursive, includes_files,             " +
                "  excludes_files, sync_on_create, sync_on_modified, sync_on_delete,            " +
                "  before_copy_script, after_copy_script,                                       " +
                "  last_update_by, last_update_time, trigger_id, create_time)                   " +
                " values(?,?,?,?,?,?,?,?,?,?,?,?,?)                                             ");                
        
        putSql("selectFileSnapshotSql", 
                " select trigger_id, router_id, file_path, file_name, last_event_type, crc32_checksum, " +
                "  file_size, file_modified_time, create_time, last_update_time, last_update_by        " +
                " from $(file_snapshot) where trigger_id=? and router_id=?                             ");
        
        putSql("updateFileSnapshotSql",                 
                " update $(file_snapshot) set   " +
                        "  last_event_type=?, crc32_checksum=?,                                 " + 
                        "  file_size=?, file_modified_time=?, last_update_time=?,               " +
                        "  last_update_by=?                                                     " +
                        " where                                                                 " +
                        "  trigger_id=? and router_id=? and file_path=? and file_name=?         ");

        putSql("insertFileSnapshotSql",                 
                " insert into $(file_snapshot) (                                                " +
                "  last_event_type, crc32_checksum,                                             " + 
                "  file_size, file_modified_time, create_time, last_update_time,                " +
                "  last_update_by, trigger_id, router_id, file_path, file_name                  " +
                " ) values(?,?,?,?,?,?,?,?,?,?,?)                                                 ");
        
        putSql("selectFileTriggerRoutersSql", 
                " select                                                                        " +
                "  tr.trigger_id as trigger_id, tr.router_id as router_id, enabled,             " +
                "  initial_load_enabled, target_base_dir, target_file_path,                     " +
                "  conflict_strategy, tr.create_time as create_time,                            " +
                "  tr.last_update_by as last_update_by, tr.last_update_time as last_update_time " +
                " from $(file_trigger_router) tr                                                ");
        
        putSql("whereTriggerRouterId", "where trigger_id=? and router_id=?");        
        
        putSql("fileTriggerRoutersForCurrentNodeWhere", " " +
                " inner join $(router) r on " +
                "  tr.router_id=r.router_id " +
                " where r.source_node_group_id=?");        
        
        putSql("updateFileTriggerRouterSql",                 
                " update $(file_trigger_router) set                                             " +
                        "  enabled=?, initial_load_enabled=?, target_base_dir=?,                " +
                        "  target_file_path=?,                                                  " +
                        "  conflict_strategy=?, last_update_by=?, last_update_time=?            " +
                " where trigger_id=? and router_id=?                                            ");

        putSql("insertFileTriggerRouterSql",                 
                " insert into $(file_trigger_router) (                                          " +
                        "  enabled, initial_load_enabled, target_base_dir,                      " +
                        "  target_file_path,                                                    " +
                        "  conflict_strategy, create_time, last_update_by,                      " +
                        "  last_update_time, trigger_id, router_id                              " +
                " ) values(?,?,?,?,?,?,?,?,?,?)                                                 ");        
        
        
        
        
    }


}
