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


package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.ITriggerRouterService;

/**
 * Background job that checks to see if triggers need to be regenerated.
 */
public class SyncTriggersJob extends AbstractJob {

    private ITriggerRouterService triggerRouterService;

    public SyncTriggersJob() {
    }

    @Override
    public long doJob() throws Exception {
        triggerRouterService.syncTriggers();
        return -1l;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }
    
    public String getClusterLockName() {
        return ClusterConstants.SYNCTRIGGERS;
    }
    
    public boolean isClusterable() {
        return true;
    }

}