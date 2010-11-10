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


package org.jumpmind.symmetric.model;

import java.util.Date;

/**
 * This is the data that changed due to a data sync trigger firing.
 *
 * 
 */
public class Data extends AbstractCsvData {

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

    /**
     * This is a reference to the triggerHistory row the trigger referred to
     * when the data event fired.
     */
    private TriggerHistory triggerHistory;

    private String channelId;

    private String transactionId;

    private String sourceNodeId;

    private String externalData;

    /**
     * This is populated by the trigger when the event happens. It will be
     * useful for research.
     */
    private Date createTime;

    public Data(long dataId, String pkData, String rowData,
            DataEventType eventType, String tableName, Date createTime,
            TriggerHistory triggerHistory, String channelId,
            String transactionId, String sourceNodeId) {
        super();
        this.dataId = dataId;
        this.pkData = pkData;
        this.rowData = rowData;
        this.eventType = eventType;
        this.tableName = tableName;
        this.createTime = createTime;
        this.triggerHistory = triggerHistory;
        this.channelId = channelId;
        this.transactionId = transactionId;
        this.sourceNodeId = sourceNodeId;
    }

    public Data(String tableName, DataEventType eventType, String rowData,
            String pkData, TriggerHistory triggerHistory, String channelId,
            String transactionId, String sourceNodeId) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowData = rowData;
        this.pkData = pkData;
        this.triggerHistory = triggerHistory;
        this.channelId = channelId;
        this.transactionId = transactionId;
        this.sourceNodeId = sourceNodeId;
    }

    public Data() {
    }

    public String[] toParsedRowData() {
        return getData("rowData", rowData);
    }

    public String[] toParsedOldData() {
        return getData("oldData", oldData);
    }

    public String[] toParsedPkData() {
        return getData("pkData", pkData);
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
        return rowData;
    }

    public void setRowData(String rowData) {
        this.rowData = rowData;
    }

    public String getPkData() {
        return pkData;
    }

    public void setPkData(String pkData) {
        this.pkData = pkData;
    }

    public String getOldData() {
        return oldData;
    }

    public void setOldData(String oldData) {
        this.oldData = oldData;
    }

    public TriggerHistory getTriggerHistory() {
        return triggerHistory;
    }

    public void setTriggerHistory(TriggerHistory triggerHistory) {
        this.triggerHistory = triggerHistory;
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