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

package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.model.NodeChannel;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
public interface IRouterContext extends ICacheContext {

    /**
     * Get the same template that is being used for inserts into data_event for routing.
     */
    public JdbcTemplate getJdbcTemplate();

    /**
     * Get the channel that is currently being routed
     */
    public NodeChannel getChannel();

    public boolean isEncountedTransactionBoundary();
    
    public void incrementStat(long amount, String name);
    
}