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
import org.jumpmind.symmetric.web.rest.model.ChannelStatus;
import org.jumpmind.symmetric.web.rest.model.Engine;
import org.jumpmind.symmetric.web.rest.model.EngineList;
import org.jumpmind.symmetric.web.rest.model.Identity;
import org.jumpmind.symmetric.web.rest.model.NodeStatus;
import org.jumpmind.symmetric.web.rest.model.RestError;
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

/**
 * REST API into the SymmetricDS Engine.
 * 
 * <p>
 * <b>General HTTP Responses to the methods:</b>
 * <ul>
 * <li>
 * ALL Methods may return the following HTTP responses.<br>
 * <br>
 * In general:<br>
 * <ul>
 * <li>HTTP 2xx = Success</li>
 * <li>HTTP 4xx = Problem on the caller (client) side</li>
 * <li>HTTP 5xx - Problem on the REST service side</li>
 * </ul>
 * ALL Methods
 * <ul>
 * <li>HTTP 401 - Unauthorized.  You have not successfully authenticated.  
 * Authentication details are in the response body.</li>
 * <li>HTTP 404 - Not Found.  You attempted to perform an operation
 * on a resource that doesn't exist.  I.E. you tried to start or stop an 
 * engine that doesn't exist. </li>
 * <li>HTTP 405 - Method Not Allowed.  I.E. you attempted a service
 * call that uses the default engine (/engine/identity vs engine/{engine}/identity)
 * and there was more than one engine found on the server.</li>  
 * <li>HTTP 500 - Internal Server Error.  Something went wrong on the
 * server / service, and we couldn't fulfill the request.  Details are in the response
 * body.</li>
 * </ul>
 * </li>
 * <li>
 * GET Methods
 * <ul>
 * <li>HTTP 200 - Success with result contained in the response body.</li>
 * <li>HTTP 204 - Success with no results.  Your GET request completed
 * successfully, but found no matching entities.</li>
 * </ul>
 * </ul>
 * </p>
 */
@Controller
public class RestService {

    @Autowired
    ServletContext context;
    /**
     * Provides a list of {@link Engine} that are configured on the node.
     * 
     * @return {@link EngineList} - Engines configured on the node
     */
    @RequestMapping(value = "/engine", method = RequestMethod.GET)
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
     * Returns the {@link Identity} for a given engine on the node.
     *   
     * @param engine - The engine name for which the action is intended.  
     * 
     * @return {@link Identity} - The identity of the engine<br>
     */
    @RequestMapping(value = "/engine/{engine}/identity", method = RequestMethod.GET)
    @ResponseBody
    public final Identity identity(
    		@PathVariable("engine") String engineName) {
    	return identityImpl(getSymmetricEngine(engineName));
    }

    /**
     * Returns the {@link Identity} for the single engine on this node.  
     * 
     * @return {@link Identity} - The identity of the engine
     */
    @RequestMapping(value = "/engine/identity", method = RequestMethod.GET)
    @ResponseBody
    public final Identity identity() {    	
    	return identityImpl(getSymmetricEngine());
    }
         
    /**
     * Loads a profile for the specified engine on the node.
     * 
     * @param engine - The engine name for which the action is intended.     
     * @param file - A file stream that contains the profile itself.
     * TODO:  put more details here on the specifics of how the file needs to be passed
     */
    @RequestMapping(value = "engine/{engine}/profile", method = RequestMethod.POST)
    @ResponseBody
    public final void loadProfile(
    		@PathVariable("engine") String actionName, String engineName,
            @RequestParam (value = "file") MultipartFile file) {
        
    	loadProfileImpl(getSymmetricEngine(engineName), file);
    }

    /**
     * Loads a profile for the single engine on the node.
     *      
     * @param file - A file stream that contains the profile itself.
     * TODO:  put more details here on the specifics of how the file needs to be passed
     */
    @RequestMapping(value = "engine/profile", method = RequestMethod.POST)
    @ResponseBody
    public final void loadProfile(@RequestParam MultipartFile file) {
    	
    	loadProfileImpl(getSymmetricEngine(), file);
    }
    
    
    /**
     * Performs a specific action on the single engine on the node.  Valid actions include:
     * <p>
     * <ul>
     * <li><b>synctriggers</b> - Creates or updates trigger instances that are defined in your 
     * synchronization scenario (i.e. defined in sym_trigger)</li>
     * <li><b>droptriggers</b> - Removes trigger instances that are defined in your synchronizationh scenario
     * <li><b>uninstall</b> - Uninstalls all SymmetricDS objects from the given node (database)</li>
     * <li><b>reinitialize</b> - Reinitializes an engine including unregistering the engine and 
     * removing all symmetric configuration, operational objects and data from the
     * node (database).  Does not remove the engine. Thus, if the engine is a Server instance,
     * when the engine starts back up, it will need configuration reloaded. If the engine is
     * a client, it will request registration again from its configured registration server.</li>
     * <li><b> - refreshcache</b> - Refreshes the cached parameters 
     * <li><b>start</b> - Starts the given engine</li>
     * <li><b>stop</b> - Stops the given engine</li>
     * </ul>
     * </p>
     * @param actionName - One of the actions listed above
     * @param force - Whether to force the action to occur regardless of activity occuring on the node
     * @return  TODO: determine whether we really want this to return an ActionResponse object (is it really needed?)
     */
    @RequestMapping(value = "/engine", method = RequestMethod.GET)
    @ResponseBody
    public final void action(
    		@RequestParam(value = "action") String actionName,    		
    		@RequestParam(required = false, value = "force") boolean force) {
    	
    	actionImpl(getSymmetricEngine(), actionName, force);
    }    
    
    
    /**
     * Performs a specific action on the specified engine on the node.  
     * @param engine - The engine name for which the action is intended.
     * @param action - The action desired
     * @param force - Whether to force the action to occur regardless of activity occuring on the node
     * @return  TODO: determine whether we reall want the return type.
     * @see #action(String, boolean)
     */
    @RequestMapping(value = "/engine/{engine}", method = RequestMethod.GET)
    @ResponseBody
    public final void action(
    		@PathVariable("engine") String engineName,
    		@RequestParam(value = "action") String actionName,    		
    		@RequestParam(required = false, value = "force") boolean force) {
    	
    	actionImpl(getSymmetricEngine(engineName), actionName, force);
    	
    }

    /**
     * Returns an overall status for the specified engine of the node.
     * 
     * @param engineName
     * @return {@link NodeStatus}
     */
    @RequestMapping(value = "/engines/{engine}/status", method = RequestMethod.GET)
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
    @RequestMapping(value = "/engines/{engine}/channelstatus", method = RequestMethod.GET)
    @ResponseBody
    public final Set<ChannelStatus> channelStatus(@PathVariable("engine") String engineName) {
        throw new RuntimeException("Test");
    }

    //***********************************************************************************************
    //TODO: stuff that should probably get moved out to some type of delegate or implementation class
    //***********************************************************************************************
    
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

    private Identity identityImpl(ISymmetricEngine engine) {
    	//TODO: implement
    	return null;
    }
    
    private void loadProfileImpl(ISymmetricEngine engine, MultipartFile file) {
    	//TODO:implement    	
    }

    private void actionImpl(ISymmetricEngine engine, String actionName, boolean force) {
		
		if (StringUtils.isNotBlank(actionName) && engine != null) {
			// this is already there (/triggers post or delete).
			if (actionName.equals("synctriggers")) {
				ITriggerRouterService triggerRouterService = engine
						.getTriggerRouterService();
				StringBuilder buffer = new StringBuilder();
				triggerRouterService.syncTriggers(buffer, force);
				//FIGURE OUT WHAT WE WANT TO DO HERE.  RESPONSE CODE IN HTTP RESPONSE AND MSG IN BODY?				
//				SyncTriggersActionResponse response = new SyncTriggersActionResponse();
//				response.setSuccess(true);
//				response.setMessage(buffer.toString());
				
			} else if (actionName.equals("uninstall")) {
				engine.uninstall();
			} else if (actionName.equals("reinitialize")) {

			} else if (actionName.equals("droptriggers")) {

			} else if (actionName.equals("start")) {

			} else if (actionName.equals("stop")) {

			}
		}
		throw new NotFoundException();
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
        }
        if (engine == null) {
            throw new NotFoundException();
        } else {
            return engine;
        }
    }
    
    protected ISymmetricEngine getSymmetricEngine() {
        SymmetricEngineHolder holder = getSymmetricEngineHolder();

        if (holder.getEngines().size() == 1) {
            return holder.getEngines().values().iterator().next();
        } else {
        	throw new NotAllowedException();
        }        
    }

}
