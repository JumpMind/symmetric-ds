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
package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.IncomingBatch;

/**
 * This extension point is called whenever a batch has completed loading but before
 * the transaction has committed.
 */
public interface IBatchListener extends IExtensionPoint {

    /**
     * If the {@link ParameterConstants#DATA_LOADER_MAX_ROWS_BEFORE_COMMIT} property is set and the max number of 
     * rows is reached and a commit is about to happen, then this method is called.
     */
    public void earlyCommit(IDataLoader loader, IncomingBatch batch);
    
    /**
     * This method is called after a batch has been successfully processed. It
     * is called in the scope of the transaction that controls the batch commit.
     */
    public void batchComplete(IDataLoader loader, IncomingBatch batch);
    
    /**
     * This method is called after the database transaction for the batch has been committed.
     */
    public void batchCommitted(IDataLoader loader, IncomingBatch batch);
    
    /**
     * This method is called after the database transaction for the batch has been rolled back.
     */
    public void batchRolledback(IDataLoader loader, IncomingBatch batch, Exception ex); 
}