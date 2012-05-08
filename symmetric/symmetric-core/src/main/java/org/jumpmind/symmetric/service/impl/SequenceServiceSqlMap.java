package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class SequenceServiceSqlMap extends AbstractSqlMap {

    public SequenceServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        putSql("getSequenceSql",
          "select sequence_name,current_value,increment_by,min_value,max_value,                    " + 
          "cycle,create_time,last_update_by,last_update_time from $(sequence) where sequence_name=?");
        
        putSql("getCurrentValueSql",
                "select current_value from $(sequence) where sequence_name=?");     
        
        putSql("updateCurrentValueSql",
                "update $(sequence) set current_value=?, last_update_time=current_timestamp " +
                "  where sequence_name=? and current_value=?                                ");          
        
        putSql("insertSequenceSql",
                "insert into $(sequence)                                               " +
                "  (sequence_name, current_value, increment_by, min_value, max_value,  " + 
                "   cycle, create_time, last_update_by, last_update_time)              " +
                "   values(?,?,?,?,?,?,current_timestamp,?,current_timestamp)         ");     
        
        putSql("maxOutgoingBatchSql", "select max(batch_id)+1 from $(outgoing_batch)");
                
       // @formatter:on
    }

}
