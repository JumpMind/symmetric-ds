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
package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

public class Batch {

	/**
	 * The batch id for the given batch
	 */
	private long batchId;
	
	/**
	 * The list of sql statements captured on the source for this batch
	 */
	private List<String> sqlStatements;
	
	/**
	 * Returns the batchId for this batch
	 * @return
	 */
	public long getBatchId() {
		return batchId;
	}
	
	/**
	 * Setter for the batch id field.
	 * @param batchId
	 */
	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}
	
	/**
	 * Returns the list of sql statements captured in this batch
	 * @return
	 */
	public List<String> getSqlStatements() {
		return sqlStatements;
	}
	
	/**
	 * Setter for the sqlStatements field
	 * @param sqlStatements
	 */
	public void setSqlStatements(List<String> sqlStatements) {
		this.sqlStatements = sqlStatements;
	}
}
