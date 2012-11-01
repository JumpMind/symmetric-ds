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
package org.jumpmind.symmetric.web.rest;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.symmetric.web.rest.model.ActionResponse;
import org.jumpmind.symmetric.web.rest.model.ChannelStatus;
import org.jumpmind.symmetric.web.rest.model.Engine;
import org.jumpmind.symmetric.web.rest.model.EngineList;
import org.jumpmind.symmetric.web.rest.model.Identity;
import org.jumpmind.symmetric.web.rest.model.NodeStatus;
import org.jumpmind.symmetric.web.rest.model.RestError;
import org.jumpmind.symmetric.web.rest.model.SyncTriggersActionResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

@Controller
public class RestService {

    @Autowired
    ServletContext context;

    /**
     * Returns the list of engine names that are configured on the node.
     * 
     * @return Set<{@link Engine> of engine names configured on the node
     */
    @RequestMapping(value = "/engines", method = RequestMethod.GET)
    @ResponseBody
    public final EngineList engine() {
        EngineList list = new EngineList();
        Collection<String> engineNames = getSymmetricEngineHolder().getEngines().keySet();
        for (String engine : engineNames) {
            list.addEngine(new Engine(engine));
        }
        return list;
    }

    /**
     * Returns the identity for the single engine on the node. If more than one
     * engine exists on the node, service will return an HTTP Status Code 405
     * (Method Not Allowed)
     * 
     * @return Identity the identity of the engine
     */
    @RequestMapping(value = "/identity", method = RequestMethod.GET)
    @ResponseBody
    public final Identity identity(@RequestParam("engine") String engineName) {
        // TODO:implement
        // ISymmetricEngine engine = getSymmetricEngine(engineName);
        // return engine.getNodeService().findIdentityNodeId();
        return null;
    }

    /**
     * Loads a profile for the specified engine on the node.
     * 
     * @param engineName
     */
    @RequestMapping(value = "profile", method = RequestMethod.POST)
    @ResponseBody
    // TODO: figure out how we will pass the file info...
    public final void loadProfile(@RequestParam("engine") String engineName,
            @RequestParam MultipartFile file) {
        System.out.println("File '" + file.getOriginalFilename() + "' uploaded successfully");
    }

    @RequestMapping(value = "/actions/{action}", method = RequestMethod.GET)
    @ResponseBody
    public final ActionResponse action(
            @RequestParam(required = false, value = "engine") String engineName,
            @PathVariable("action") String actionName,
            @RequestParam(required = false, value = "force") boolean force) {
        ISymmetricEngine engine = getSymmetricEngine(engineName);
        if (StringUtils.isNotBlank(actionName) && engine != null) {
            if (actionName.equals("synctriggers")) {
                ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
                StringBuilder buffer = new StringBuilder();
                triggerRouterService.syncTriggers(buffer, force);
                SyncTriggersActionResponse response = new SyncTriggersActionResponse();
                response.setSuccess(true);
                response.setMessage(buffer.toString());
                return response;
            } else if (actionName.equals("uninstall")) {
                engine.uninstall();
                ActionResponse response = new ActionResponse("SymmetricDS uninstalled");
                return response;
            } else if (actionName.equals("reinitialize")) {

            } else if (actionName.equals("start")) {

            } else if (actionName.equals("stop")) {

            }
        }
        throw new NotFoundException();
    }

    /**
     * Drops SymmetricDS triggers for the specified engine and table on the
     * node.
     * 
     * @param engineName
     * @param tableName
     */
    @RequestMapping(value = "/tables/{table}/triggers", method = RequestMethod.DELETE)
    @ResponseBody
    public final void dropTrigger(@RequestParam("engine") String engineName,
            @PathVariable("table") String tableName) {
        // TODO: Implementation
    }

    /**
     * Drops all SymmetricDS triggers for the specified engine on the node.
     * 
     * @param engineName
     */
    @RequestMapping(value = "/triggers", method = RequestMethod.DELETE)
    @ResponseBody
    public final void dropTrigger(@PathVariable("engine") String engineName) {
        // TODO: Implementation
    }

    /**
     * Creates SymmetricDS triggers for the specified engine and table on the
     * node.
     * 
     * @param engineName
     * @param tableName
     */
    @RequestMapping(value = "/tables/{table}/triggers", method = RequestMethod.POST)
    @ResponseBody
    public final void syncTrigger(@RequestParam("engine") String engineName,
            @PathVariable("table") String tableName) {
        // TODO: Implementation
    }

    /**
     * Reinitializes the specified engine on the node. This includes:
     * <ul>
     * <li>Uninstalling all SymmetricDS objects from the database</li>
     * <li>Reregistering the node</li>
     * <li>Initial load (if configured)</li>
     * <li>Reverse initial load (if configured)</li>
     * </ul>
     * 
     * @param engineName
     */
    @RequestMapping(value = "/reinitialize/engines/{engine}", method = RequestMethod.POST)
    @ResponseBody
    public final void initialize(@PathVariable("engine") String engineName) {
        // TODO: Implementation
    }

    /**
     * Returns an overall status for the specified engine of the node.
     * 
     * @param engineName
     * @return {@link NodeStatus}
     */
    @RequestMapping(value = "/engines/{engine}/node/status", method = RequestMethod.GET)
    @ResponseBody
    public final NodeStatus nodeStatus(@PathVariable("engine") String engineName) {
        // TODO: Implementation
        return new NodeStatus();
    }

    /**
     * Returns status of each channel for the specified engine of the node.
     * 
     * @param engineName
     * @return Set<{@link ChannelStatus}>
     */
    @RequestMapping(value = "/engines/{engine}/channel/status", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> channelStatus(@PathVariable("engine") String engineName) {
        throw new RuntimeException("Test");
    }

    /**
     * Uninstalls all SymmetricDS objects from the database for the specified
     * engine of the node.
     * 
     * @param engineName
     */
    @RequestMapping(value = "/engines/{engine}", method = RequestMethod.DELETE)
    @ResponseBody
    public final void unintstall(@PathVariable("engine") String engineName) {
        // TODO: Implementation
    }

    /**
     * Starts SymmetricDS for the node.
     */
    @RequestMapping(value = "/start", method = RequestMethod.POST)
    @ResponseBody
    public final void start() {
        // TODO: Implementation
    }

    /**
     * Stops SymmetricDS for the node.
     */
    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    @ResponseBody
    public final void stop() {
        // TODO: Implementation
    }

    /**
     * Refreshes the cache for the node.
     */
    @RequestMapping(value = "/cache", method = RequestMethod.PUT)
    @ResponseBody
    public final void refreshCache() {
        // TODO: Implementation
    }

    // TODO: reloadtable
    // TODO: reloadnode

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public RestError handleError(Exception ex, HttpServletRequest req) {
        int httpErrorCode = 500;
        Annotation annotation = ex.getClass().getAnnotation(ResponseStatus.class);
        if (annotation != null) {
            httpErrorCode = ((ResponseStatus) annotation).value().value();
        }
        return new RestError(ex, httpErrorCode);
    }

    protected SymmetricEngineHolder getSymmetricEngineHolder() {
        SymmetricEngineHolder holder = (SymmetricEngineHolder) context
                .getAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        if (holder == null) {
            throw new NotFoundException();
        }
        return holder;
    }

    protected ISymmetricEngine getSymmetricEngine(String engineName) {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();
        
        ISymmetricEngine engine = null;        
        if (StringUtils.isNotBlank(engineName)) {
            engine = holder.getEngines().get(engineName);
        } else if (holder.getEngines().size() > 0) {
            engine = holder.getEngines().values().iterator().next();
        }
        
        if (engine == null) {
            throw new NotFoundException();
        } else {
            return engine;
        }
    }

}
