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
import java.util.List;
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
        List<FileTriggerRouter> triggerRouters = fileSyncService
                .getFileTriggerRoutersForCurrentNode(triggerId);
        for (FileTriggerRouter fileTriggerRouter : triggerRouters) {
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
        }

        return nodeIds;
    }

}
