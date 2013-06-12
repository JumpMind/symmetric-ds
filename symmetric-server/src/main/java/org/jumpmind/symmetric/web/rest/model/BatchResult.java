/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.web.rest.model;

public class BatchResult {

	/**
	 * The node ID for which batches are being acknowledged
	 */
	private String nodeId;
	
	/**
	 * The batch ID for the batch being acknowledged
	 */
	private Long batchId;
	
	/**
	 * The status of the batch after it was loaded.  Either "OK" or "ER"
	 */
	private String status;

	/**
	 * The sqlCode that resulted if the batch being loaded is in error  
	 */
	private int sqlCode;
	
	/**
	 * The sqlState that resulted if the batch being loaded is in error
	 */
	private String sqlState;	
	
	/**
	 * A description of the status. This is particularly important if the status is "ER". 
	 * In error status the status description should contain relevant information about the 
	 * error on the client including SQL Error Number and description
	 */
	private String statusDescription;
	
	public BatchResult(String nodeId, long batchId, boolean success) {
	    this.status = success ? "OK" : "ER";
	    this.nodeId = nodeId;
	    this.batchId = batchId;
    }
	
	public BatchResult() {
    }
		
	/**
	 * Returns the nodeId for the given batch result
	 * @return
	 */
	public String getNodeId() {
		return nodeId;
	}
	
	/**
	 * Sets the nodeId for the given batch result
	 * @param nodeId
	 */
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	
	/**
	 * Gets the batch id for the given batch result
	 * @return
	 */
	public Long getBatchId() {
		return batchId;
	}
	
	/**
	 * Sets the batch id for the given batch result
	 * @param batchId
	 */
	public void setBatchId(Long batchId) {
		this.batchId = batchId;
	}
	
	/**
	 * Gets the status for the given batch result
	 * @return
	 */
	public String getStatus() {
		return status;
	}
	/**
	 * Sets the status for the given batch result
	 * @param status
	 */
	public void setStatus(String status) {
		this.status = status;
	}
	/**
	 * Gets the status description for the given batch result
	 * @return
	 */
	public String getStatusDescription() {
		return statusDescription;
	}
	/**
	 * Sets the status description for the given batch result
	 * @param statusDescription
	 */
	public void setStatusDescription(String statusDescription) {
		this.statusDescription = statusDescription;
	}

	/**
	 * Gets the sqlCode for the batch 
	 * @return
	 */
	public int getSqlCode() {
		return sqlCode;
	}

	/**
	 * Sets the sqlCode for the batch
	 * @param sqlCode
	 */
	public void setSqlCode(int sqlCode) {
		this.sqlCode = sqlCode;
	}

	/**
	 * gets the sqlState for the batch
	 * @return
	 */
	public String getSqlState() {
		return sqlState;
	}

	/**
	 * sets the sqlState for the batch
	 * @param sqlState
	 */
	public void setSqlState(String sqlState) {
		this.sqlState = sqlState;
	}

}
