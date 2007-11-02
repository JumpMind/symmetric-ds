/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

public interface IDbDialect {

    public void initTrigger(DataEventType dml, Trigger config,
            TriggerHistory audit, String tablePrefix, Table table);

    public void removeTrigger(String schemaName, String triggerName);

    public void initConfigDb(String tablePrefix);

    public Platform getPlatform();
    
    public String getName();
    
    public String getVersion();

    public boolean doesTriggerExist(String schema, String tableName, String triggerName);

    public Table getMetaDataFor(String schema, final String tableName, boolean useCache);

    public String getTransactionTriggerExpression();

    public String createInitalLoadSqlFor(Node client, Trigger config);
    
    public String createPurgeSqlFor(Node node, Trigger trig);
    
    public String createCsvDataSql(Trigger trig, String whereClause);
    
    public String createCsvPrimaryKeySql(Trigger trig, String whereClause);

    public boolean isCharSpacePadded();
    
    public boolean isCharSpaceTrimmed();
    
    public boolean isEmptyStringNulled();
    
    public void purge();
    
    public SQLErrorCodeSQLExceptionTranslator getSqlErrorTranslator();
    
    public void disableSyncTriggers();

    public void enableSyncTriggers();
    
    public String getDefaultSchema();
    
    public int getStreamingResultsFetchSize();
    
    public JdbcTemplate getJdbcTemplate();
    
}
