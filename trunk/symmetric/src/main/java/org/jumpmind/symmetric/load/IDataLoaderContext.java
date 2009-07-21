/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.load;

import java.util.Map;

import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.BinaryEncoding;

public interface IDataLoaderContext {

    public long getBatchId();

    public String getNodeId();

    public String getTableName();

    public String getChannelId();

    public String getVersion();

    public boolean isSkipping();

    public String[] getColumnNames();

    /**
     * Old data is only sent when the sync_column_level feature is enabled in the trigger configuration. It was needed for
     * that feature, and there is some overhead to sending old data, so their is a flag to enable it. 
     * <p/>
     * <code>
     * update sym_trigger set sync_column_level = 1, last_updated_time = current_timestamp where trigger_id = ?
     * </code>
     * 
     * @return an array of the previous values of the row that is being data sync'd.
     */
    public String[] getOldData();

    public String[] getKeyNames();

    public int getColumnIndex(String columnName);

    public Table[] getAllTablesProcessed();

    public Map<String, Object> getContextCache();

    public TableTemplate getTableTemplate();

    public BinaryEncoding getBinaryEncoding();

    public Object[] getObjectValues(String[] values);

    public Object[] getObjectKeyValues(String[] values);

    public Object[] getOldObjectValues();

}