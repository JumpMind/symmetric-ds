/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.load;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

/**
 * @author Eric Long <erilong@users.sourceforge.net>,
 */
public class DataLoaderContext implements IDataLoaderContext {

    static final ILog log = LogFactory.getLog(DataLoaderContext.class);

    private String sourceNodeId;
    
    private String targetNodeId;

    private String tableName;
    
    private String schemaName;
    
    private String catalogName;

    private String channelId;

    private long batchId;

    private boolean isSkipping;

    private transient Map<String, TableTemplate> tableTemplateMap;

    private TableTemplate tableTemplate;

    private Map<String, Object> contextCache = new HashMap<String, Object>();

    private BinaryEncoding binaryEncoding = BinaryEncoding.NONE;
    
    private INodeService nodeService;
    
    private transient Node sourceNode;
    
    private transient Node targetNode;

    public DataLoaderContext(INodeService nodeService) {
        this();
        this.nodeService = nodeService;
    }
    
    public DataLoaderContext() {
        this.tableTemplateMap = new HashMap<String, TableTemplate>();
    }
    
    public Node getNode() {
          return getSourceNode();
    }
    
    public Node getSourceNode()
    {
        if (sourceNode == null) {
            this.sourceNode = nodeService != null && sourceNodeId != null ? nodeService.findNode(this.sourceNodeId) : null; 
        }
        return this.sourceNode;      
    }
    
    public Node getTargetNode()
    {
        if (targetNode == null) {
            this.targetNode = nodeService != null && targetNodeId != null ? nodeService.findIdentity() : null; 
        }
        return this.targetNode;      
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

    public int getKeyIndex(String columnName) {
        String[] columnNames = tableTemplate.getKeyNames();
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

    public String getSourceNodeId() {
        return sourceNodeId;
    }
    
    public String getNodeId() {
        return getSourceNodeId();
    }


    public void setSourceNodeId(String sourceNodeId) {        
        this.sourceNodeId = sourceNodeId;
        this.sourceNode = null;
    }
    
    public void setNodeId(String sourceNodeId)
    {
        setSourceNodeId(sourceNodeId);
    }
    
    
    public String getTargetNodeId() {
        return targetNodeId;
    }
    
    
    public void setTargetNodeId(String targetNodeId) {        
        this.targetNodeId = targetNodeId;
        this.targetNode = null;
    }
    
    
    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }
    
    public String getCatalogName() {
        return catalogName;
    }
    
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    
    public String getSchemaName() {
        return schemaName;
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