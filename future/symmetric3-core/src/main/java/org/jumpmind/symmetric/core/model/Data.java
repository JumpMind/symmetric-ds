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
 * under the License. 
 */
package org.jumpmind.symmetric.core.model;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.symmetric.csv.CsvUtils;

/**
 * This is the data that changed due to a data sync trigger firing.
 */
public class Data extends AbstractCsvData implements Serializable {

    private static final String OLD_DATA = "oldData";

    private static final String PK_DATA = "pkData";

    private static final String ROW_DATA = "rowData";

    private static final long serialVersionUID = 1L;

    /**
     * Primary key
     */
    private long dataId;

    private String tableName;

    private DataEventType eventType;

    /**
     * Comma delimited row data.
     */
    private String rowData;

    /**
     * Comma delimited primary key data.
     */
    private String pkData;

    /**
     * To support column-level sync and conflict resolution. Comma delimited old
     * row data from an update.
     */
    private String oldData;

    private String channelId;

    private String transactionId;

    private String sourceNodeId;

    private String externalData;

    /**
     * This is populated by the trigger when the event happens. It will be
     * useful for research.
     */
    private Date createTime;

    public Data(String tableName, DataEventType eventType, String rowData) {
        this(-1, null, rowData, eventType, tableName, null, null, null, null);
    }

    public Data(String tableName, DataEventType eventType, String pkData, String rowData) {
        this(-1, pkData, rowData, eventType, tableName, null, null, null, null);
    }

    public Data(long dataId, String pkData, String rowData, DataEventType eventType,
            String tableName, Date createTime, String channelId, String transactionId,
            String sourceNodeId) {
        this.dataId = dataId;
        this.pkData = pkData;
        this.rowData = rowData;
        this.eventType = eventType;
        this.tableName = tableName;
        this.createTime = createTime;

        this.channelId = channelId;
        this.transactionId = transactionId;
        this.sourceNodeId = sourceNodeId;
    }

    public Data(String tableName, DataEventType eventType, String rowData, String pkData,
            String channelId, String transactionId, String sourceNodeId) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowData = rowData;
        this.pkData = pkData;
        this.channelId = channelId;
        this.transactionId = transactionId;
        this.sourceNodeId = sourceNodeId;
    }

    public Data() {
    }

    public void putParsedRowData(String[] tokens) {
        putData(ROW_DATA, tokens);
    }

    public void putParsedOldData(String[] tokens) {
        putData(OLD_DATA, tokens);
    }

    public void putParsedPkData(String[] tokens) {
        putData(PK_DATA, tokens);
    }

    public String[] toParsedRowData() {
        return getData(ROW_DATA, rowData);
    }

    public String[] toParsedOldData() {
        return getData(OLD_DATA, oldData);
    }

    public String[] toParsedPkData() {
        return getData(PK_DATA, pkData);
    }

    public void clearPkData() {
        this.pkData = null;
        this.removeData(PK_DATA);
    }

    public long getDataId() {
        return dataId;
    }

    public void setDataId(long dataId) {
        this.dataId = dataId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DataEventType getEventType() {
        return eventType;
    }

    public void setEventType(DataEventType eventType) {
        this.eventType = eventType;
    }

    public String getRowData() {
        if (rowData == null) {
            String[] data = toParsedRowData();
            if (data != null && data.length > 0) {
                rowData = CsvUtils.escapeCsvData(data);
            }
        }
        return rowData;
    }

    public void setRowData(String rowData) {
        this.rowData = rowData;
    }

    public String getPkData() {
        if (pkData == null) {
            String[] data = toParsedPkData();
            if (data != null && data.length > 0) {
                pkData = CsvUtils.escapeCsvData(data);
            }
        }
        return pkData;
    }

    public void setPkData(String pkData) {
        this.pkData = pkData;
    }

    public String getOldData() {
        if (oldData == null) {
            String[] data = toParsedOldData();
            if (data != null && data.length > 0) {
                oldData = CsvUtils.escapeCsvData(data);
            }
        }
        return oldData;
    }

    public void setOldData(String oldData) {
        this.oldData = oldData;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public void setSourceNodeId(String sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public String getExternalData() {
        return externalData;
    }

    public void setExternalData(String externalData) {
        this.externalData = externalData;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

}