package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class ClusterServiceSqlMap extends AbstractSqlMap {

    public ClusterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("aquireLockSql" ,"" + 
"update $(lock) set locking_server_id=?, lock_time=? where                   " + 
"  lock_action=? and (lock_time is null or lock_time < ? or locking_server_id=?)   " );

        putSql("releaseLockSql" ,"" + 
"update $(lock) set locking_server_id=null, lock_time=null, last_lock_time=current_timestamp, last_locking_server_id=?   " + 
"  where lock_action=? and locking_server_id=?                                                                                 " );

        putSql("insertLockSql" ,"" + 
"insert into $(lock) (lock_action) values(?)   " );

        putSql("findLocksSql" ,"" + 
"select lock_action, locking_server_id, lock_time, last_locking_server_id, last_lock_time   " + 
"  from $(lock)                                                                       " );

    }

}