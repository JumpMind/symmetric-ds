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
package org.jumpmind.symmetric.route;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IRouterService;

public class FileSyncDataRouter extends AbstractDataRouter {

    public static final String ROUTER_TYPE = "filesync";

    private ISymmetricEngine engine;

    public FileSyncDataRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        Set<String> nodeIds = new HashSet<String>();
        IFileSyncService fileSyncService = engine.getFileSyncService();
        IRouterService routerService = engine.getRouterService();

        Map<String, String> newData = getNewDataAsString(null, dataMetaData,
                engine.getSymmetricDialect());
        String triggerId = newData.get("TRIGGER_ID");
        String routerId = newData.get("ROUTER_ID");
        String sourceNodeId = newData.get("LAST_UPDATE_BY");
        
        if (triggerId == null) {
            Map<String, String> oldData = getOldDataAsString(null, dataMetaData,
                    engine.getSymmetricDialect());
            triggerId = oldData.get("TRIGGER_ID");
            routerId = oldData.get("ROUTER_ID");            
            sourceNodeId = oldData.get("LAST_UPDATE_BY");
        }
        FileTriggerRouter fileTriggerRouter = fileSyncService.getFileTriggerRouter(
                triggerId, routerId);
        if (fileTriggerRouter != null) {
            Router router = fileTriggerRouter.getRouter();
            Map<String, IDataRouter> routers = routerService.getRouters();
            IDataRouter dataRouter = null;
            if (StringUtils.isNotBlank(router.getRouterType())) {
                dataRouter = routers.get(router.getRouterType());
            }

            if (dataRouter == null) {
                dataRouter = routers.get("default");
            }

            if (context instanceof ChannelRouterContext) {
                ((ChannelRouterContext) context).addUsedDataRouter(dataRouter);
            }
            nodeIds.addAll(dataRouter.routeToNodes(context, dataMetaData, nodes, false));
            nodeIds.remove(sourceNodeId);
        } else {
            log.error(
                    "Could not find a trigger router with a trigger_id of {} and a router_id of {}.  The file snapshot will not be routed",
                    triggerId, routerId);
        }
        return nodeIds;
    }

}
