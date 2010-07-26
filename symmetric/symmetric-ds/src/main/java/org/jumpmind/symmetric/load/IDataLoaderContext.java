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

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.model.Node;

public interface IDataLoaderContext extends ICacheContext {

    public long getBatchId();

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

    /**
     * @return an array of the previous values of the row that is being data sync'd.
     */
    public String[] getOldData();

    public String[] getKeyNames();

    public int getColumnIndex(String columnName);
    
    public int getKeyIndex(String columnName);

    public Table[] getAllTablesProcessed();

    public TableTemplate getTableTemplate();

    public BinaryEncoding getBinaryEncoding();

    public Object[] getObjectValues(String[] values);

    public Object[] getObjectKeyValues(String[] values);

    public Object[] getOldObjectValues();

}