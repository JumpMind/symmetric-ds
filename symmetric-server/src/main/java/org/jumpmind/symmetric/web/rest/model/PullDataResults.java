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

import java.util.List;

public class PullDataResults {

	/**
	 * The number of batches that were returned in this pull request
	 */
	private int nbrBatches;
	
	/**
	 * The actual list of {@link Batch}
	 */
	private List<Batch> batches;

	/**
	 * Returns the number of batches that were returned for this pull request
	 * @return
	 */
	public int getNbrBatches() {
		return nbrBatches;
	}

	/**
	 * Setter for the nbr batches field
	 * @param nbrBatches
	 */
	public void setNbrBatches(int nbrBatches) {
		this.nbrBatches = nbrBatches;
	}

	/**
	 * Gets the list of batches for this pull request
	 * @return
	 */
	public List<Batch> getBatches() {
		return batches;
	}

	/**
	 * Setter for the list of batches field
	 * @param batches
	 */
	public void setBatches(List<Batch> batches) {
		this.batches = batches;
	}

}
