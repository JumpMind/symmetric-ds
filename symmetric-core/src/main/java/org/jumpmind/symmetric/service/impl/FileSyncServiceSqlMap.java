package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class FileSyncServiceSqlMap extends AbstractSqlMap {

    public FileSyncServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        
        putSql("selectFileTriggersSql", 
                " select trigger_id, base_dir, recursive, includes_files, excludes_files,      " +
        		"       sync_on_create, sync_on_modified, sync_on_delete, create_time,         " +
        		"       last_update_by, last_update_time                                       " +
        		" from $(file_trigger)                                                         ");
        
        putSql("whereTriggerId", "where trigger_id=?");
        
        putSql("updateFileTriggerSql",                 
                " update $(file_trigger) set base_dir=?, recursive=?, includes_files=?,        " +
        		"  excludes_files=?, sync_on_create=?, sync_on_modified=?, sync_on_delete=?,   " +
        		"  last_update_by=?, last_update_time=? where trigger_id=?                     ");

        putSql("insertFileTriggerSql",                 
                " insert into $(file_trigger) (base_dir, recursive, includes_files,            " +
                "  excludes_files, sync_on_create, sync_on_modified, sync_on_delete,           " +
                "  last_update_by, last_update_time, trigger_id, create_time)                  " +
                " values(?,?,?,?,?,?,?,?,?,?,?)                                                ");
        
        putSql("selectFileSnapshotSql", 
                " select trigger_id, file_path, file_name, last_event_type, crc32_checksum,     " +
                "  file_size, file_modified_time, create_time, last_update_time, last_update_by " +
                " from $(file_snapshot) where trigger_id=?                                      ");
        
    }


}
