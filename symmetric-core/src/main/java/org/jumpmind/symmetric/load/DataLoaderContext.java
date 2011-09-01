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
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @see IDataLoaderContext
 */
public class DataLoaderContext implements IDataLoaderContext {

    static final ILog log = LogFactory.getLog(DataLoaderContext.class);

    private String sourceNodeId;
    
    private String targetNodeId;

    private String tableName;
    
    private String schemaName;
    
    private String catalogName;

    private String channelId;

    private IncomingBatch batch;

    private boolean isSkipping;

    private transient Map<String, TableTemplate> tableTemplateMap;

    private TableTemplate tableTemplate;

    private Map<String, Object> contextCache = new HashMap<String, Object>();

    private BinaryEncoding binaryEncoding = BinaryEncoding.NONE;
    
    private INodeService nodeService;
    
    private NodeGroupLink nodeGroupLink;
    
    private transient Node sourceNode;
    
    private transient Node targetNode;
    
    private JdbcTemplate jdbcTemplate;

    public DataLoaderContext(INodeService nodeService, JdbcTemplate jdbcTemplate) {
        this.tableTemplateMap = new HashMap<String, TableTemplate>();
        this.nodeService = nodeService;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
    
    public NodeGroupLink getNodeGroupLink() {
        if (nodeGroupLink == null && sourceNodeId != null) {
            Node sourceNode = nodeService.findNode(sourceNodeId);
            Node targetNode = getTargetNode();
            if (sourceNode != null && targetNode != null) {
                nodeGroupLink = new NodeGroupLink(sourceNode.getNodeGroupId(), getTargetNode()
                        .getNodeGroupId());
            }
        }
        return nodeGroupLink;
    }
    
    public Node getNode() {
          return getSourceNode();
    }   
    
    public Node getSourceNode() {
        if (sourceNode == null && nodeService != null && sourceNodeId != null) {
            this.sourceNode = nodeService.findNode(this.sourceNodeId);
        }
        return this.sourceNode;
    }
    
    public Node getTargetNode() {
        if (targetNode == null && nodeService != null) {
            this.targetNode = nodeService.findIdentity();
        }
        return this.targetNode;
    }
    
    public TableTemplate getTableTemplate() {
        return tableTemplate;
    }
    
    private final String getFullQualifiedTableName() {
        return catalogName + "." + schemaName + "." + tableName;
    }

    public void setTableTemplate(TableTemplate tableTemplate) {
        this.tableTemplate = tableTemplate;
        tableTemplateMap.put(getFullQualifiedTableName(), tableTemplate);
    }

    public int getFilteredColumnIndex(String columnName) {
        String[] columnNames = tableTemplate.getFilteredColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnName)) {
                return i;
            }
        }
        return -1;        
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

    public void clearBatch() {
        this.batch = null;    
    }
    
    public IncomingBatch getBatch() {
        return batch;
    }

    public void setBatchId(long batchId) {
        this.batch = new IncomingBatch(batchId, this);
        isSkipping = false;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }
    
    public String getNodeId() {
        return getSourceNodeId();
    }
    
    public long getBatchId() {
        return batch != null ? batch.getBatchId() : -1;
    }


    public void setSourceNodeId(String sourceNodeId) {        
        this.sourceNodeId = sourceNodeId;
        this.sourceNode = null;
        this.nodeGroupLink = null;
        getNodeGroupLink();
    }
    
    @Deprecated
    public void setNodeId(String sourceNodeId) {
        setSourceNodeId(sourceNodeId);
    }
    
    
    public String getTargetNodeId() {
        return targetNodeId;
    }
    
    
    public void setTargetNodeId(String targetNodeId) {        
        this.targetNodeId = targetNodeId;
        this.targetNode = null;
        this.nodeGroupLink = null;
        getTargetNode();
        getNodeGroupLink();
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
    }

    public void chooseTableTemplate() {
        this.tableTemplate = tableTemplateMap.get(getFullQualifiedTableName());
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
    
    public String[] getFilteredColumnNames() {
        return tableTemplate.getFilteredColumnNames();
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