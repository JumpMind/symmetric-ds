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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.BinaryEncoding;

public class DataLoaderContext implements IDataLoaderContext {

    static final ILog log = LogFactory.getLog(DataLoaderContext.class);

    private String version;

    private String nodeId;

    private String tableName;

    private String channelId;

    private long batchId;

    private boolean isSkipping;

    private transient Map<String, TableTemplate> tableTemplateMap;

    private TableTemplate tableTemplate;

    private Map<String, Object> contextCache = new HashMap<String, Object>();

    private BinaryEncoding binaryEncoding = BinaryEncoding.NONE;

    public DataLoaderContext() {
        this.tableTemplateMap = new HashMap<String, TableTemplate>();
    }

    public TableTemplate getTableTemplate() {
        return tableTemplate;
    }

    public void setTableTemplate(TableTemplate tableTemplate) {
        this.tableTemplate = tableTemplate;
        tableTemplateMap.put(getTableName(), tableTemplate);
    }

    public int getColumnIndex(String columnName) {
        String[] columnNames = tableTemplate.getColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public Table[] getAllTablesProcessed() {
        Collection<TableTemplate> templates = this.tableTemplateMap.values();
        Table[] tables = new Table[templates.size()];
        int i = 0;
        for (TableTemplate table : templates) {
            tables[i++] = table.getTable();
        }
        return tables;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
        isSkipping = false;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
        this.tableTemplate = tableTemplateMap.get(tableName);
    }

    public String[] getOldData() {
        return this.tableTemplate != null ? this.tableTemplate.getOldData() : null;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isSkipping() {
        return isSkipping;
    }

    public void setSkipping(boolean isSkipping) {
        this.isSkipping = isSkipping;
    }

    public String[] getColumnNames() {
        return tableTemplate.getColumnNames();
    }

    public void setColumnNames(String[] columnNames) {
        tableTemplate.setColumnNames(columnNames);
    }

    public void setOldData(String[] oldData) {
        tableTemplate.setOldData(oldData);
    }

    public String[] getKeyNames() {
        return tableTemplate.getKeyNames();
    }

    public void setKeyNames(String[] keyNames) {
        tableTemplate.setKeyNames(keyNames);
    }

    /**
     * This is a cache that is available for the lifetime of a batch load. It
     * can be useful for storing data from the filter for customization
     * purposes.
     */
    public Map<String, Object> getContextCache() {
        return contextCache;
    }

    public BinaryEncoding getBinaryEncoding() {
        return binaryEncoding;
    }

    public void setBinaryEncoding(BinaryEncoding binaryEncoding) {
        this.binaryEncoding = binaryEncoding;
    }

    public void setBinaryEncodingType(String encoding) {
        try {
            this.binaryEncoding = BinaryEncoding.valueOf(encoding);
        } catch (Exception ex) {
            log.warn("EncodingUnsupported", encoding);
        }
    }

    public Object[] getOldObjectValues() {
        String[] oldData = this.getOldData();
        if (oldData != null) {
            return tableTemplate.getObjectValues(this, oldData);
        } else {
            return null;
        }
    }

    public Object[] getObjectValues(String[] values) {
        return tableTemplate.getObjectValues(this, values);
    }

    public Object[] getObjectKeyValues(String[] values) {
        return tableTemplate.getObjectKeyValues(this, values);
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

}
