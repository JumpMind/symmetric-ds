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

package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;


/**
 * This service is responsible for routing data to specific nodes and managing
 * the batching of data to be delivered to each node.
 * 
 * @since 2.0
 */
public interface IRouterService {

    public long routeData();
 
    public long getUnroutedDataCount();
    
    public boolean shouldDataBeRouted(SimpleRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad);
 
    public void addDataRouter(String name, IDataRouter dataRouter);
    
    public void addBatchAlgorithm(String name, IBatchAlgorithm algorithm);
    
    /**
     * Get a list of available batch algorithms that can be used for the different channels
     */
    public List<String> getAvailableBatchAlgorithms();
    
    public Map<String, IDataRouter> getRouters();
    
    public void stop ();
    
    public void destroy();
}