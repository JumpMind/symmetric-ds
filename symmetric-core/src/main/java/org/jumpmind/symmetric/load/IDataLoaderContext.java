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

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A working context used by the data loader
 */
public interface IDataLoaderContext extends ICacheContext {

    public IncomingBatch getBatch();

    /**
     * Returns the node id on which the data was extracted; i.e., the source node id.
     * @return source node id
     * @deprecated Replaced by getSourceNodeId
     * @see #getSourceNodeId()
     */
    public String getNodeId();
    
    /**
     *  Returns the node id on which the data was extracted; i.e., the source node id
     * @return source node id
     */
    public String getSourceNodeId();
    
    /**
     * Returns the node id on which the data is being loaded; i.e., the target node id
     * @return target node id
     */
    public String getTargetNodeId();
    
    public NodeGroupLink getNodeGroupLink();
    
    /**
     *Returns the node on which the data was extracted; i.e., the source node
     * @return source node 
     * @deprecated Replaced by getSourceNode
     * @see #getSourceNode()
     * 
     */
    public Node getNode();
    
    /**
     * Returns the node on which the data was extracted; i.e., the source node
     * @return source node
     */
    public Node getSourceNode();
    
    /**
     * Returns the node on which the data is being loaded; i.e., the target node
     * @return target node 
     */
    public Node getTargetNode();

    public String getSchemaName();

    public String getCatalogName();

    public String getTableName();

    public String getChannelId();

    public boolean isSkipping();

    public String[] getColumnNames();
    
    public String[] getFilteredColumnNames();

    /**
     * @return an array of the previous values of the row that is being data sync'd.
     */
    public String[] getOldData();

    public String[] getKeyNames();

    public int getColumnIndex(String columnName);
    
    public int getFilteredColumnIndex(String columnName);
    
    public int getKeyIndex(String columnName);

    public Table[] getAllTablesProcessed();

    public TableTemplate getTableTemplate();

    public BinaryEncoding getBinaryEncoding();

    public Object[] getObjectValues(String[] values);

    public Object[] getObjectKeyValues(String[] values);

    public Object[] getOldObjectValues();
    
    /**
     * @return a {@link JdbcTemplate} that wraps the connection being used to do the data load.
     */
    public JdbcTemplate getJdbcTemplate();

}